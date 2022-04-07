module MuSearch
  # This class represents index definitions as defined in the configuration file of mu-search
  # in the config file you will find these definitions on the the keyword "types"
  # It was created mostly the abstract some of the complexity that composite and sub types introduce
  class IndexDefinition
    attr_reader :name, :on_path, :rdf_type, :properties, :mappings, :settings
    attr_reader :sub_types, :composite_types

    def initialize(
          name:,
          on_path:,
          rdf_type: nil,
          composite_types: nil,
          sub_types: nil,
          properties:,
          mappings: nil,
          settings: nil
        )
      @name = name
      @on_path = on_path
      @rdf_type = rdf_type
      @sub_types = ! sub_types.nil? && sub_types.kind_of?(Array) ? sub_types : []
      @composite_types = ! composite_types.nil? && composite_types.kind_of(Array) ? composite_types : []
      @properties = properties
      @mappings = mappings
      @settings = settings

      if is_composite_index? and is_regular_index?
        raise "invalid type definition for #{type}. Composite indexes can't have a rdf_type"
      end
    end

    def self.from_json_def(hash)
      IndexDefinition.new(
        name: hash["type"],
        on_path: hash["on_path"],
        rdf_type: hash["rdf_type"],
        composite_types: hash["composite_types"],
        sub_types: hash["sub_types"],
        properties: hash["properties"],
        mappings: hash["mappings"],
        settings: hash["settings"]
      )
    end

    def type
      @name
    end

    def has_sub_types?
      return @sub_types.length > 0
    end

    # an index definition is for a composite index if it is composed of several types
    # this means the actual definition 
    def is_composite_index?
      return @composite_types.length > 0
    end

    # an index defition is for a regular index if an rdf_type is specified
    def is_regular_index?
      return ! @rdf_type.nil?
    end

    def to_s
      "Index definition: {@name: #{name}, @on_path: #{on_path}}"
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
      when "sub_types"
        @sub_types
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
  end
end
