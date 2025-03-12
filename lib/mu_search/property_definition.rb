module MuSearch
  class PropertyDefinition
    PROPERTY_TYPES = ["simple", "nested", "attachment", "language-string", "lambert-72"]
    attr_reader :name, :type, :rdf_type, :path, :pipeline, :sub_properties

    def initialize(name: , path:,  type: "auto", rdf_type: nil, sub_properties:)
      raise "invalid type" unless PROPERTY_TYPES.include?(type)
      raise "path needs to be an array" unless path.is_a?(Array)
      @name = name
      @path = path.is_a?(String) ? [path] : path
      @type = type

      if type == "nested"
        @rdf_type = rdf_type
        @sub_properties = sub_properties
      end
    end

    def self.from_json_config(name, config)
      type = "simple"
      rdf_type = sub_properties = pipeline = nil
      if config.is_a?(Hash)
        path = config["via"].is_a?(Array) ? config["via"] : [config["via"]]
        if config.key?("attachment_pipeline")
          type = "attachment"
        elsif config.key?("properties")
          type = "nested"
          sub_properties = config["properties"].map do |subname, subconfig|
            from_json_config(subname, subconfig)
          end
          rdf_type = config["rdf_type"]
        elsif config.key?("type") 
            if config["type"] == "language-string"
                type = "language-string"
            elsif config["type"] == "lambert-72"
                type = "lambert-72"
            end
        end
      elsif config.is_a?(Array)
        path = config
      else
        path = [config]
      end

      PropertyDefinition.new(
        name: name,
        type: type,
        path: path,
        rdf_type: rdf_type,
        sub_properties: sub_properties,
      )
    end

  end
end
