# frozen_string_literal: true

require_relative 'update_handler'
require_relative 'document_builder'

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
    #   - uri: String URI of the entity which needs an update.
    #   - index_types: Array of index types where the document needs to be updated
    #   - update_type: Type of the update (:update or :delete)
    #
    # Note: since updates may have been queued for a while, the update type is not taken into account.
    #       The current state of the triplestore is taken as the source of truth.
    #       If the document exists and is accessible in the triplestore for a set of allowed groups,
    #            the document gets updated in the corresponding search index
    #       If the document doesn't exist (anymore) or is not accessible in the triplestore for a set of allowed groups,
    #            the document is removed from the corresponding search index
    def handler(uri, index_types, _update_type)
      index_types.each do |index_type|
        @logger.debug("UPDATE HANDLER") { "Updating document <#{uri}> in indexes for type '#{index_type}'" }

        indexes = @index_manager.indexes[index_type]
        indexes.each do |_, index|
          index_definition = @type_definitions[index_type]
          rdf_types = index_definition.related_rdf_types
          allowed_groups = index.allowed_groups
          # check if document exists for any of the types related to the (composite) index
          if document_exists_for?(allowed_groups, uri, rdf_types)
            build_and_upsert_document(allowed_groups, uri, index_definition, index)
          else
            @logger.debug("UPDATE HANDLER") do
              "Resource <#{uri}> (type #{index_definition.name}) not accessible or already removed in triplestore for allowed groups #{allowed_groups}. Removing document from Elasticsearch index #{index.name} as well."
            end
            remove_document(uri, index)
          end
        end
      end
    end

    private

    def build_and_upsert_document(allowed_groups, uri, index_definition, index)
      @logger.info("UPDATE HANDLER") do
        "Document <#{uri}> needs to be updated in index #{index.name} for '#{index_definition.name}' and allowed groups #{allowed_groups}"
      end
      @sparql_connection_pool.with_authorization(allowed_groups) do |sparql_client|
        document_builder = MuSearch::DocumentBuilder.new(
          tika: @tika,
          sparql_client: sparql_client,
          attachment_path_base: @attachment_path_base,
          logger: @logger
        )
        document = document_builder.build_document_for_index(uri: uri, index_definition: index_definition)
        @logger.debug("UPDATE_HANDLER") { document.pretty_inspect }
        @elasticsearch.upsert_document(index.name, uri, document)
      end
    end

    def remove_document(document_id, index)
      begin
        @elasticsearch.delete_document(index.name, document_id)
      rescue StandardError => e
        # TODO: check type of error and log warning if needed
        @logger.info("UPDATE HANDLER") { "Failed to delete document #{document_id} from index #{index.name}" }
        @logger.debug("UPDATE_HANDLER") { e }
      end
    end

    ##
    # assumes rdf_types is an array
    def document_exists_for?(allowed_groups, uri, rdf_types)
      @sparql_connection_pool.with_authorization(allowed_groups) do |sparql_client|
        rdf_types_string = rdf_types.map { |type| Mu::sparql_escape_uri(type) }.join(',')
        sparql_client.query "ASK {#{Mu::sparql_escape_uri(uri)} a ?type. filter(?type in(#{rdf_types_string})) }"
      end
    end
  end
end
