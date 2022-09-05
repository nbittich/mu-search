require_relative 'update_handler'

module MuSearch
  ##
  # the automatic update handler is a service that executes updates or deletes on indexes.
  # when a document needs to be udpated the handler will fetch the required information from the triplestore
  # and insert that data into the correct index
  # when a document needs to be deleted it will verify that the document no longer exists in the triplestore
  # and if so remove it from the index
  # this handler takes the configured allowed_groups of an index into account
  class AutomaticUpdateHandler < MuSearch::UpdateHandler

    ##
    # creates an automatic update handler
    def initialize(elasticsearch:, tika:, sparql_connection_pool:, search_configuration:, **args)
      @elasticsearch = elasticsearch
      @tika = tika
      @sparql_connection_pool = sparql_connection_pool
      @type_definitions = search_configuration[:type_definitions]
      @attachment_path_base = search_configuration[:attachment_path_base]

      super(search_configuration: search_configuration, **args)
    end

    ##
    # Update all documents relating to a particular uri and a series of
    # types.
    #
    #   - document_id: String URI of the entity which needs an update.
    #   - index_types: Array of index types where the document needs to be updated
    #   - update_type: Type of the update (:update or :delete)
    #
    # Note: since updates may have been queued for a while, the update type is not taken into account.
    #       The current state of the triplestore is taken as the source of truth.
    #       If the document exists and is accessible in the triplestore for a set of allowed groups,
    #            the document gets updated in the corresponding search index
    #       If the document doesn't exist (anymore) or is not accessible in the triplestore for a set of allowed groups,
    #            the document is removed from the corresponding search index
    def handler(document_id, index_types, update_type)
      index_types.each do |index_type|
        @logger.debug("UPDATE HANDLER") { "Updating document <#{document_id}> in indexes for type '#{index_type}'" }

        indexes = @index_manager.indexes[index_type]

        indexes.each do |_, index|
          rdf_types = @type_definitions[index_type].related_rdf_types

          allowed_groups = index.allowed_groups
          if document_exists_for?(allowed_groups, document_id, rdf_types)
            @logger.info("UPDATE HANDLER") { "Document <#{document_id}> needs to be updated in index #{index.name} for '#{index_type}' and allowed groups #{allowed_groups}" }
            document_builder = MuSearch::DocumentBuilder.new(
              tika: @tika,
              sparql_client: sparql_client,
              attachment_path_base: @attachment_path_base,
              logger: @logger
            @sparql_connection_pool.with_authorization(allowed_groups) do |sparql_client|
              definition = nil
              properties = definition["properties"]
              document = document_builder.fetch_document_to_index(uri: document_id, properties: properties)
              @elasticsearch.upsert_document index.name, document_id, document
            end
          else
            @logger.info("UPDATE HANDLER") { "Document <#{document_id}> not accessible or already removed in triplestore for allowed groups #{allowed_groups}. Removing document from Elasticsearch index #{index.name} as well." }
            begin
              @elasticsearch.delete_document index.name, document_id
            rescue
              # TODO check type of error and log warning if needed
              @logger.info  ("UPDATE HANDLER") { "Failed to delete document #{document_id} from index #{index.name}" }
            end
          end
        end
      end
    end

    private
    def expand_composite_type_definition composite_type_def
      simple_types = composite_type_def["composite_types"]
      simple_types.map do |simple_type|
        simple_type_def = @type_definitions[simple_type]
        properties = composite_type_def["properties"].map do |composite_prop|
          property_name = composite_prop["name"]
          mapped_name = composite_prop["mappings"] && composite_prop["mappings"][simple_type]
          mapped_name = composite_prop["name"] if mapped_name.nil?
          property_def = simple_type_def["properties"][mapped_name]
          [property_name, property_def]
        end
        {
          "type" => simple_type,
          "rdf_type" => simple_type_def["rdf_type"],
          "sub_types" => simple_type_def["sub_types"],
          "properties" => Hash[properties]
        }
      end
    end

    ##
    # assumes rdf_types is an array
    def document_exists_for?(allowed_groups, document_id, rdf_types)
      @sparql_connection_pool.with_authorization(allowed_groups) do |sparql_client|
        rdf_types_string = rdf_types.map{ |type| sparql_escape_uri(type)}.join(',')
        sparql_client.query "ASK {#{sparql_escape_uri(document_id)} a ?type. filter(?type in(#{rdf_types_string})) }"
      end
    end
  end
end
