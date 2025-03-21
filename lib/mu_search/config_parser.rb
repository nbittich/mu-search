require_relative './index_definition.rb'

module MuSearch
  module ConfigParser

    ##
    # Parse the configuration file and environment variables.
    # Fallback to a default configuration value if none is provided.
    # Environment variables take precedence over the JSON file.
    ##
    def self.parse(path)
      default_configuration = {
        batch_size: 100,
        common_terms_cutoff_frequency: 0.001,
        attachment_path_base: "/data",
        eager_indexing_groups: [],
        update_wait_interval_minutes: 1,
        number_of_threads: 1,
        enable_raw_dsl_endpoint: false
      }

      json_config = JSON.parse(File.read(path))
      config = default_configuration.clone

      # the following settings can come from either ENV or the json file
      # ENV is capitalized, we ignore empty strings from ENV and nil values from both
      [
        { name: "batch_size", parser: :parse_integer },
        { name: "max_batches", parser: :parse_integer },
        { name: "persist_indexes", parser: :parse_boolean },
        { name: "enable_raw_dsl_endpoint", parser: :parse_boolean },
        { name: "automatic_index_updates", parser: :parse_boolean },
        { name: "attachments_path_base", parser: :parse_string },
        { name: "common_terms_cutoff_frequency", parser: :parse_float },
        { name: "update_wait_interval_minutes", parser: :parse_float },
        { name: "number_of_threads", parser: :parse_integer }
      ].each do |setting|
        name = setting[:name]
        value = self.send(setting[:parser], ENV[name.upcase], json_config[name])
        config[name.to_sym] = value unless value.nil?
      end

      self.validate_config(json_config)

      # the following settings can only be configured via the json file
      config[:default_index_settings] = json_config["default_settings"] || {}
      if json_config["eager_indexing_groups"]
        config[:eager_indexing_groups] = json_config["eager_indexing_groups"]
      end
      config[:ignored_allowed_groups] = json_config["ignored_allowed_groups"] || []
      config[:type_definitions] = Hash[MuSearch::IndexDefinition.from_json_config(json_config["types"])]
      config
    end

    def self.parse_string(*possible_values)
      as_type(*possible_values) do |val|
        val.to_s
      end
    end

    def self.parse_string_array(*possible_values)
      as_type(*possible_values) do |val|
        val.each(&:to_s)
      end
    end

    def self.parse_float(*possible_values)
      as_type(*possible_values) do |val|
        Float(val)
      end
    end

    def self.parse_integer(*possible_values)
      as_type(*possible_values) do |val|
        Integer(val)
      end
    end

    def self.parse_boolean(*possible_values)
      as_type(*possible_values) do |val|
        if val.kind_of?(String) && ! val.strip.empty?
          ["true", "True", "TRUE"].include?(val)
        else
          val
        end
      end
    end

    ##
    # will return the first non nil value which was correctly returned by the provided block
    # usage:
    #  as_type("a", "number", "of", "values") do |value|
    #    Float(value)
    #  end
    #
    def self.as_type(*possible_values, &block)
      while possible_values.length > 0
        value = possible_values.shift
        begin
          unless value.nil?
            return yield(value)
          end
        end
      end
    end

    def self.validate_config(json_config)
      errors = []
      if ! json_config.has_key?("persist_indexes") || ! json_config["persist_indexes"]
        Mu::log.warn("CONFIG_PARSER") { "persist_indexes is disabled, indexes will be removed from elastic on restart!" }
      end
      if json_config.has_key?("eager_indexing_groups")
        errors = errors.concat(self.validate_eager_indexing_groups(json_config["eager_indexing_groups"]))
      end
      if json_config.has_key?("types")
        errors = errors.concat(self.validate_type_definitions(json_config["types"]))
      else
        errors << "no type definitions specified, expected field 'types' not found"
      end
      if json_config.has_key?("ignored_allowed_groups")
        errors = errors.concat(self.validate_ignored_allowed_groups(json_config["ignored_allowed_groups"]))
      end
      if errors.length > 0
        Mu::log.error("CONFIG_PARSER") { errors.join("\n") }
        raise "invalid config"
      end
    end

    def self.validate_ignored_allowed_groups(groups)
      errors = []
      if ! groups.kind_of?(Array)
        errors << "ignored_allowed_groups should be an array"
      else
        groups.each do |group|
          if ! group.kind_of?(Hash)
            errors << "ignored_allowed_group is not an object: #{group.inspect}"
          else
            if ! group.has_key?("name") && group.has_key?("variables")
              errors << "ignored_allowed_group should have both name and variables set: #{group.inspect}"
            end
          end
        end
      end
      errors
    end

    ##
    # basic validations of typedefinitions
    #
    def self.validate_type_definitions(type_definitions)
      errors = []

      types = type_definitions.map { |t| t["type"] }
      double_keys = types.select { |e| types.count(e) > 1 } # not very performant, but should be small array anyway
      if double_keys.length > 0
        errors << "the following types are defined more than once: #{double_keys}"
      end

      paths = type_definitions.map { |t| t["on_path"] }
      double_keys = paths.select { |e| paths.count(e) > 1 }
      if double_keys.length > 0
        errors << "the following paths are defined more than once: #{double_keys}"
      end

      required_keys = ["type", "properties", "on_path"]
      type_definitions.each do |type|
        required_keys.each do |key|
          unless type.has_key?(key)
            errors << "invalid type definition for #{type["type"]}, missing key #{key}"
          end
        end

        if type["rdf_type"].kind_of?(Array)
          Mu::log.warn("CONFIG_PARSER") { "#{type["type"]} specifies multiple rdf types, this is experimental!" }
          if type["rdf_type"].length == 0
            errors << "#{type["type"]} has doesn't specify any rdf_type, the array is empty."
          end
        end

        unless type["rdf_type"] || type["composite_types"]
          errors << "type definition for #{type["type"]} must specify rdf_type or composite_types"
        end

        if type.has_key?("composite_types")
          Mu::log.warn("CONFIG_PARSER") { "#{type["type"]} is a composite type, support for composite types is experimental!"}
          errors.concat(validate_composite_type(type, types))
        end

        if type.has_key?("mappings")
          unless type["mappings"].has_key?("properties")
            errors << "type definition for #{type["type"]} has an index specific mapping, but the mapping does not have the properties field."
          end
        else
          Mu::log.warn("CONFIG_PARSER") { "field mappings not set for type #{type["type"]}, you may want to add an index specific mapping." }
        end
      end
      errors
    end

    def self.validate_composite_type(type, types)
      errors = []
      unless type["composite_types"].is_a?(Array)
        errors << "composite type #{type["type"]} is not correctly specified field 'composite_types' should be an array"
      end
      undefined_types = type["composite_types"].select{ |type| ! types.include?(type)}
      if undefined_types.length > 0
        errors << "composite type #{type["type"]} refers to type(s) #{undefined_types} which don't exist"
      end
      if type["properties"].kind_of?(Array)
        type["properties"].each do |prop, value|
          unless prop.has_key?("name")
            errors << "composite type #{type["type"]} has an invalid property: properties of a composite type should have a field 'name'"
          end
        end
      else
        errors << "composite type #{type["type"]}: properties should be an array"
      end

      errors
    end
    ##
    # do some basic config validation to make debugging a faulty eager_indexing config easier
    #
    def self.validate_eager_indexing_groups(groups)
      errors = []

      groups.each do |group|
        unless group.kind_of?(Array)
          errors << "invalid eager indexing groups, each group should be an array. #{group.inspect} is not"
        end
        group.each do |access_right|
          unless access_right["name"] && access_right["variables"] && access_right["variables"].kind_of?(Array)
            errors << "invalid eager indexing group: #{group.inspect}."
          end
        end

        has_wildcards = group.map { |access_right | access_right["variables"] }.flatten!.any? "*"
        if group.length > 1 and has_wildcards
          errors << "Eager indexing group with wildcard variables may contain only 1 access right. #{group.inspect} contains #{group.length}."
        end
      end

      errors
    end
  end
end
