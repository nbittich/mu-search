require_relative 'update_handler'
require_relative 'document_builder'
require '/usr/src/app/sinatra_template/utils' # provided by template

module MuSearch
  ##
  # the automatic update handler is a service that executes updates or deletes on indexes.
  # when a document needs to be udpated the handler will fetch the required information from the triplestore
  # and insert that data into the correct index
  # when a document needs to be deleted it will verify that the document no longer exists in the triplestore
  # and if so remove it from the index
  # this handler takes the configured allowed_groups of an index into account
  class AutomaticUpdateHandler < MuSearch::UpdateHandler
    include ::SinatraTemplate::Utils

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
          index_definition = @type_definitions[index_type]
          rdf_types = index_definition.related_rdf_types
          allowed_groups = index.allowed_groups
          # check if document exists for any of the types related to the (composite) index
          if document_exists_for?(allowed_groups, document_id, rdf_types)
            @logger.info("UPDATE HANDLER") { "Document <#{document_id}> needs to be updated in index #{index.name} for '#{index_type}' and allowed groups #{allowed_groups}" }
              @sparql_connection_pool.with_authorization(allowed_groups) do |sparql_client|
                document_builder = MuSearch::DocumentBuilder.new(
                  tika: @tika,
                  sparql_client: sparql_client,
                  attachment_path_base: @attachment_path_base,
                  logger: @logger
                )
                if index_definition.is_composite_index?
                  # for composite indexes check for each sub type if it matches and only if it does build the document
                  # if multiple subindexes are matched, merge the documents
                  # TODO: verify if this is desired behaviour
                  merged_document = {}
                  relevant_sub_indexes_for(document_id, index_definition.composite_types, allowed_groups).each do |sub_definition|
                    properties = sub_definition.properties
                    document = document_builder.fetch_document_to_index(uri: document_id, properties: properties)
                    merged_document = smart_merge(merged_document, document)
                  end
                  @logger.debug ("UPDATE_HANDLER") { merged_document.pretty_inspect}
                  @elasticsearch.upsert_document index.name, document_id, merged_document
                else
                  properties = index_definition.properties
                  document = document_builder.fetch_document_to_index(uri: document_id, properties: properties)
                  @logger.debug ("UPDATE_HANDLER") { document.pretty_inspect}
                  @elasticsearch.upsert_document index.name, document_id, document
                end
              end
          else
            @logger.info("UPDATE HANDLER") { "Document <#{document_id}> (type #{index_type}) not accessible or already removed in triplestore for allowed groups #{allowed_groups}. Removing document from Elasticsearch index #{index.name} as well." }
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

    ##
    # select sub indexes matching the type(s) of the provided resource
    def relevant_sub_indexes_for(uri, composite_types, allowed_groups)
      @sparql_connection_pool.with_authorization(allowed_groups) do |sparql_client|
        types = sparql_client.query( "SELECT DISTINCT ?type WHERE { #{sparql_escape_uri(uri)} a ?type}").map{ |result| result["type"].to_s }
        composite_types.select{ |sub_definition| (sub_definition.related_rdf_types & types).length > 0 }
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

    ##
    # smart_merge document
    # 
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
