module MuSearch
  # This class represents index definitions as defined in the configuration file of mu-search
  # in the config file you will find these definitions on the the keyword "types"
  # It was created mostly the abstract some of the complexity that composite and sub types introduce
  class IndexDefinition
    attr_reader :name, :on_path, :rdf_type, :properties, :mappings, :settings
    # composite types are fake/intermediate index definitions
    attr_reader :composite_types
    def initialize(
          name:,
          on_path:,
          rdf_type: nil,
          composite_types: nil,
          properties:,
          mappings: nil,
          settings: nil
        )
      @name = name
      @on_path = on_path
      unless rdf_type.nil?
        @rdf_type = rdf_type.kind_of?(Array) ? rdf_type : [rdf_type]
      end
      @composite_types = ! composite_types.nil? && composite_types.is_a?(Array) ? composite_types : []
      @properties = properties
      @mappings = mappings
      @settings = settings
      @property_path_cache = Hash.new { |hash, key| hash[key] = Set.new  }
      build_property_paths
      if is_composite_index? and is_regular_index?
        raise "invalid type definition for #{type}. Composite indexes can't have a rdf_type"
      end
    end

    def self.create_composite_sub_definitions(composite_definition, definitions)
      sub_def_names = composite_definition["composite_types"]
      sub_definitions = definitions.map do |index_definition|
        name = index_definition["type"]
        next unless sub_def_names.include?(name)
        properties = Hash.new
        composite_definition["properties"].each do |composite_prop|
          property_name = composite_prop["name"]
          mapped_name = composite_prop.dig('mappings', name)
          if mapped_name
            properties[property_name] = index_definition.dig("properties", mapped_name)
          else
            properties[property_name] = index_definition.dig("properties", property_name)
          end
        end

        CompositeSubIndexDefinition.new(
          name: name,
          rdf_type: index_definition["rdf_type"],
          properties: properties
        )
      end
      sub_definitions.reject{ |sub| sub.nil?  }
    end

    # builds a tuples mapping the index name to the full definition for all provided types
    # expects all types as param
    def self.from_json_config(all_definitions)
      all_definitions.collect do |definition|
        name = definition["type"]
        composite_types = []
        if definition["composite_types"]
          composite_types = create_composite_sub_definitions(definition, all_definitions)
          composite_types.each do |definition|
            ensure_uuid_in_properties definition.properties
          end
        else
          # ensure uuid is included because it may be used for folding
          ensure_uuid_in_properties definition["properties"]
        end
        index_definition = IndexDefinition.new(
          name: name,
          on_path: definition["on_path"],
          rdf_type: definition["rdf_type"],
          composite_types: composite_types,
          properties: definition["properties"],
          mappings: definition["mappings"],
          settings: definition["settings"]
        )
        [name, index_definition]
      end
    end

    def self.ensure_uuid_in_properties properties
      properties["uuid"] = ["http://mu.semte.ch/vocabularies/core/uuid"] unless properties.key?("uuid")
      properties.each do |(key, value)|
        property_definition = PropertyDefinition.from_json_config(key, value)
        if property_definition.type == "nested"
          ensure_uuid_in_properties value["properties"]
        end
      end
    end

    def type
      @name
    end

    def has_multiple_types?
      return @rdf_type.kind_of?(Array) && @rdf_type.length > 0
    end

    # checks if there is any overlap between the rdf types used in this definition and the provided rdf_types
    # always returns false for composite indexes
    def contains_any_type_of(resource_rdf_types)
      if is_composite_index?
        false
      else
        ! (self.rdf_types & resource_rdf_types).empty?
      end
    end

    # an index definition is for a composite index if it is composed of several types
    # this means the actual definition
    def is_composite_index?
      return @composite_types.kind_of?(Array) && @composite_types.length > 0
    end

    # an index defition is for a regular index if an rdf_type is specified
    def is_regular_index?
      return ! @rdf_type.nil?
    end

    def to_s
      "Index definition: {@name: #{name}, @on_path: #{on_path}}"
    end

    # lists rdf types defined on this index, or on any subindex for composite indexes
    def related_rdf_types
      if is_regular_index?
        rdf_type
      else
        composite_types.map(&:related_rdf_types).flatten
      end
    end

    def matches_type?(type)
      related_rdf_types.include?(type)
    end

    def matches_property?(property)
      @property_path_cache.keys.include?(property) || @property_path_cache.keys.include?("^#{property}")
    end

    def full_property_paths_for(property)
      if matches_property?(property)
        @property_path_cache[property] + @property_path_cache["^#{property}"]
      else
        []
      end
    end

    # allow the index definition to be used as a hash
    # this is provided for backwards compatibility
    # which is why it also uses the json keys for some fields
    def [](name)
      case name
      when "type"
        @name
      when "rdf_type"
        @rdf_type
      when "on_path"
        @on_path
      when "composite_types"
        @composite_types
      when "properties"
        @properties
      when "mappings"
        @mappings
      when "settings"
        @settings
      else
        raise ArgumentError.new("#{name} is not accessible on #{self}")
      end
    end

    private
    def build_property_paths
      if is_regular_index?
        build_property_paths_for_properties(properties)
      else
        composite_types.each do |sub_definition|
          build_property_paths_for_properties(sub_definition.properties)
        end
      end
    end

    def build_property_paths_for_properties(properties)
      property_definitions = properties.map { |key, cfg| PropertyDefinition.from_json_config(key, cfg) }
      build_property_paths_for_property_definitions(property_definitions)
    end

    def build_property_paths_for_property_definitions(property_definitions, root_path = [])
      property_definitions.map do |definition|
        path = root_path + definition.path

        # Recursively walk down the nested object definition
        if definition.type == "nested"
          build_property_paths_for_property_definitions(definition.sub_properties, path)
        end

        # Add cache entry for each predicate the path consists of
        path.each do |predicate|
          @property_path_cache[predicate] << path
        end
      end
    end
  end

  # class to represent a sub index of a composite index
  # this applies the composite definition on the sub index.
  # it's only meant to be used by the document_builder for constructing a document
  class CompositeSubIndexDefinition
    attr_reader :name, :rdf_type, :properties
    def initialize(name:, rdf_type:, properties:)
      @name = name
      unless rdf_type.nil?
        @rdf_type = rdf_type.kind_of?(Array) ? rdf_type : [rdf_type]
      end
      @properties = properties
    end

    def related_rdf_types
      @rdf_type
    end
  end
end
