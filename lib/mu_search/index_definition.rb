module MuSearch
  # This class represents index definitions as defined in the configuration file of mu-search
  # in the config file you will find these definitions on the the keyword "types"
  # It was created mostly the abstract some of the complexity that composite and sub types introduce
  class IndexDefinition
    attr_reader :name, :on_path, :rdf_type, :properties, :mappings, :settings
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
      @rdf_type = rdf_type.kind_of?(Array) ? rdf_type : [rdf_type]
      @composite_types = ! composite_types.nil? && composite_types.kind_of(Array) ? composite_types : []
      @properties = properties
      @mappings = mappings
      @settings = settings

      if is_composite_index? and is_regular_index?
        raise "invalid type definition for #{type}. Composite indexes can't have a rdf_type"
      end
    end

    def self.from_json_def(hash)
      IndexDefinition.new(upd
        name: hash["type"],
        on_path: hash["on_path"],
        rdf_type: hash["rdf_type"],
        composite_types: hash["composite_types"],
        properties: hash["properties"],
        mappings: hash["mappings"],
        settings: hash["settings"]
      )
    end

    def type
      @name
    end

    def has_multiple_types?
      return @type.kind_of?(Array) && @type.length > 0
    end

    # checks if there is any overlap between the rdf types used in this definition and the provided rdf_types
    # always returns false for composite indexes
    def contains_any_type_of(resource_rdf_types)
      if is_composite_index?
        false
      else
        ! (this.rdf_types & resource_rdf_types).empty?
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
        this.rdf_type
      else
        this.composite_types.map(&:related_rdf_types).flatten
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
  end
end
