# frozen_string_literal: true

require '/usr/src/app/sinatra_template/utils' # provided by template
require_relative './property_definition'
module MuSearch
  ##
  # This class is responsible for building JSON documents from an IndexDefinition
  class DocumentBuilder
    def initialize(tika:, sparql_client:, attachment_path_base:, logger:)
      @tika = tika
      @sparql_client = sparql_client # authorized client from connection pool
      @attachment_path_base = attachment_path_base
      @cache_path_base = "/cache/"
      @logger = logger
    end

    ##
    # Builds a document to index for the given resource URI and index_definition
    def build_document_for_index(uri:, index_definition:)
      if index_definition.is_composite_index?
        fetch_document_for_composite_index(uri: uri, index_definition: index_definition)
      else
        fetch_document_to_index(uri: uri, properties: index_definition.properties)
      end
    end

    private

    # Constructs a document for a regular index
    #   - uri: URI of the resource to fetch
    #   - properties: Array of raw properties as configured in the search config
    def fetch_document_to_index(uri: nil, properties: nil)
      property_definitions = properties.map do |key, prop_config|
        PropertyDefinition.from_json_config(key, prop_config)
      end
      construct_document_to_index(uri: uri, definitions: property_definitions)
    end

    ##
    # construct a document for a composite index
    #
    # this is quite similar to a regular index,
    # but the provided resource (uri) could have multiple types and thus match multiple subtypes
    # in some cases this means different property paths may be mapped on the same field in the document
    # this is handled here by merging those documents as good as possible
    def fetch_document_for_composite_index(uri:, index_definition:)
      raise "document_builder: expected a composite index" unless index_definition.is_composite_index?

      merged_document = {}
      relevant_sub_indexes_for(uri, index_definition.composite_types).each do |sub_definition|
        properties = sub_definition.properties
        document = fetch_document_to_index(uri: uri, properties: properties)
        merged_document = smart_merge(merged_document, document)
      end
      merged_document
    end

    # Constructs a document to index for the given resource URI and property definitions.
    #
    # The properties are queried from the triplestore using the DocumentBuilder's SPARQL client
    # which is configured with the appropriate mu-auth-allowed-groups.
    #
    # This is your one-stop shop to query all data to index a document.
    #   - uri: URI of the resource to fetch
    #   - definitions: Array of property definitions based on the properties configured in the search config
    def construct_document_to_index(uri: nil, definitions: property_definitions)
      # We will collect all the properties in one go through a construct
      # query.  For this to work we first create the information in a
      # metamodel which we then use to create a CONSTRUCT query and
      # later to extract the right information from the CONSTRUCT query.

      # Construct a meta model for the information we want to fetch.
      # This is just a list with some information for each different
      # property we want to fetch.

      # Build meta
      property_query_info = definitions.map.with_index do |definition, idx|
        predicate_string = MuSearch::SPARQL.make_predicate_string(definition.path)
        construct_uri = "http://mu.semte.ch/vocabularies/ext/#{definition.name}"

        Hash({
          construct_uri: construct_uri,
          sparql_property_path: predicate_string,
          sparql_where_variable: "?var__#{idx}",
          property_definition: definition
        })
      end

      # Build sparql query
      escaped_value_prop = "<http://mu.semte.ch/vocabularies/ext/value>"
      escaped_source_uri = SinatraTemplate::Utils.sparql_escape_uri(uri)

      construct_portion_list = property_query_info.map do |info|
        escaped_construct_uri = SinatraTemplate::Utils.sparql_escape_uri(info[:construct_uri]),
        "#{escaped_construct_uri} #{escaped_value_prop} #{info[:sparql_where_variable]}."
      end

      where_portion_list = property_query_info.map do |info|
        "#{escaped_source_uri} #{info[:sparql_property_path]} #{info[:sparql_where_variable]}."
      end

      query = <<SPARQL
      CONSTRUCT {
        #{construct_portion_list.join("\n    ")}
      }
      WHERE {
      {
        #{where_portion_list.join("\n    \} UNION \{\n      ")}
      }
    }
SPARQL

      # Collect the result into an ES document
      results = @sparql_client
                  .query(query)
                  .group_by { |triple| triple.s.to_s }

      key_value_tuples = property_query_info.map do |info|
        matching_triples = results[info[:construct_uri]] || []
        matching_values = matching_triples.map { |triple| triple.o }
        definition = info[:property_definition]

        if definition.type == "simple"
          index_value = build_simple_property(matching_values)
        elsif definition.type == "language-string"
          index_value = build_language_property(matching_values)
        elsif definition.type == "attachment"
          index_value = build_file_field(matching_values)
        elsif definition.type == "nested"
          index_value = build_nested_object(matching_values, definition.sub_properties)
        else
          raise "Unsupported property type #{definition.type} for property #{definition.name}. Property will not be handled by the document builder"
        end

        [definition.name, denumerate(index_value)]
      end

      Hash[key_value_tuples]
    end

    # Get the array of values to index for a given SPARQL result set of simple values.
    # Values are constructed based on the literal datatype.
    def build_simple_property(values)
      values.collect do |value|
        case value
        when RDF::Literal::Integer
          value.to_i
        when RDF::Literal::Double
          value.to_f
        when RDF::Literal::Decimal
          value.to_f
        when RDF::Literal::Boolean
          value.true?
        when RDF::Literal::Time
          value.to_s
        when RDF::Literal::Date
          value.to_s
        when RDF::Literal::DateTime
          value.to_s
        when RDF::Literal
          value.to_s
        else
          value.to_s
        end
      end
    end

    # Get the array of values to index as language strings for a given SPARQL result set
    #
    # Returns an object mapping languages to their values
    # {
    #   default: ["My label with lang tag"],
    #   nl: ["Dutch label"],
    #   fr: ["French label", "Another label"]
    # }
    def build_language_property(literals)
      language_map = Hash.new {|hash, key| hash[key] = [] }
      literals.collect do |literal|
        value = literal.to_s
        if literal.language?
          language = literal.language.to_s
          language_map[language] << value
        else
          language_map["default"] << value
        end
      end
      [language_map]
    end

    # Get the array of objects to be indexed for a given SPARQL result set
    # of related resources configured to be indexed as nested object.
    # The properties to be indexed for the nested object are passed as an argument.
    def build_nested_object(related_resources, nested_prop_definitions)
      related_resources.collect do |resource_uri|
        nested_document = construct_document_to_index(uri: resource_uri, definitions: nested_prop_definitions)
        nested_document.merge({ uri: resource_uri })
      end
    end

    # Get the array of file objects to be indexed for a given set of file URIs.
    #
    # The file object to index currently contains the following properties:
    # - content: text content of the file
    # This list may be extended with additional metadata in the future.
    def build_file_field(file_uris)
      file_uris.collect do |file_uri|
        file_path = File.join(@attachment_path_base, file_uri.to_s.sub("share://", ""))
        if File.exist?(file_path)
          file_size = File.size(file_path)
          if file_size < ENV["MAXIMUM_FILE_SIZE"].to_i
            content = extract_text_content(file_path)
          else
            @logger.warn("INDEXING") do
              "File #{file_path} (#{file_size} bytes) exceeds the allowed size of #{ENV['MAXIMUM_FILE_SIZE']} bytes. File content will not be indexed."
            end
            content = nil
          end
        else
          @logger.warn("INDEXING") { "File #{file_path} not found. File content will not be indexed." }
          content = nil
        end
        { content: content }
      end
    end

    # Extract the text content of the file at the given path using Tika.
    # Use a previously cached result if one is available.
    # On successfull processing, returns the extracted text content.
    # Otherwise, returns nil.
    #
    # Entries are cached using the file hash as key.
    def extract_text_content(file_path)
      begin
        file = File.open(file_path, "rb")
        blob = file.read
        file.close
        file_hash = Digest::SHA256.hexdigest blob
        cached_file_path = "#{@cache_path_base}#{file_hash}"
        if File.exists? cached_file_path
          text_content = File.open(cached_file_path, mode: "rb", encoding: 'utf-8') do |file|
            @logger.debug("TIKA") { "Using cached result #{cached_file_path} for file #{file_path}" }
            file.read
          end
        else
          text_content = @tika.extract_text file_path, blob
          if text_content.nil?
            @logger.info("TIKA") { "Received empty result from Tika for file #{file_path}. File content will not be indexed." }
            # write emtpy file to make cache hit on next run
            File.open(cached_file_path, "w") {}
          else
            @logger.debug("TIKA") { "Extracting text from #{file_path} and storing result in #{cached_file_path}" }
            File.open(cached_file_path, "w") do |file|
              file.puts text_content.force_encoding("utf-8").unicode_normalize
            end
          end
        end
        text_content
      rescue Errno::ENOENT, IOError => e
        @logger.warn("TIKA") { "Error reading file at #{file_path} to extract content. File content will not be indexed." }
        @logger.warn("TIKA") { e.full_message }
        nil
      rescue StandardError => e
        @logger.warn("TIKA") { "Failed to extract content of file #{file_path}. File content will not be indexed." }
        @logger.warn("TIKA") { e.full_message }
        nil
      end
    end

    # Utility function to denumerate the given array value.
    # I.e.
    # - returns nil if the given array is empty
    # - returns a single value if the given array only contains one element
    # - returns the array value if the given array contains mulitple elements
    def denumerate(value)
      case value.length
      when 0 then nil
      when 1 then value.first
      else value
      end
    end

    ##
    # select sub indexes matching the type(s) of the provided resource
    def relevant_sub_indexes_for(uri, composite_types)
      types = @sparql_client.query( "SELECT DISTINCT ?type WHERE { #{Mu::sparql_escape_uri(uri)} a ?type}").map{ |result| result["type"].to_s }
      composite_types.select{ |sub_definition| (sub_definition.related_rdf_types & types).length > 0 }
    end

    ##
    # smart_merge document
    def smart_merge(document_a, document_b)
      document_a.merge(document_b) do |key, a_val, b_val|
        if a_val.nil?
          b_val
        elsif b_val.nil?
          a_val
        elsif a_val.is_a?(Array) && b_val.is_a?(Array)
          a_val.concat(b_val).uniq
        elsif a_val.is_a?(Array) && (b_val.is_a?(String) || b_val.is_a?(Integer) || b_val.is_a?(Float))
          [*a_val, b_val].uniq
        elsif a_val.is_a?(Hash)
          # b_val must also be a hash
          smart_merge(a_val, b_val)
        elsif  b_val.is_a?(Array) && (b_val.is_a?(String) || b_val.is_a?(Integer) || b_val.is_a?(Float))
          # a_val can not be nil or an array, so must be a simple value
          [*b_val, a_val].uniq
        elsif (a_val.is_a?(String) || a_val.is_a?(Integer) || a_val.is_a?(Float)) &&
              (b_val.is_a?(String) || b_val.is_a?(Integer) || b_val.is_a?(Float))
          [a_val,b_val].uniq
        else
          raise "smart_merge: Invalid combo #{a_val.inspect} and #{b_val.inspect} can not be merged"
        end
      end
    end
  end
end
