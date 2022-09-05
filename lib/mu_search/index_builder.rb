require 'parallel'
require 'concurrent'

module MuSearch
  class IndexBuilder

    def initialize(logger:, elasticsearch:, tika:, sparql_connection_pool:, search_index:, search_configuration: )
      @logger = logger
      @elasticsearch = elasticsearch
      @tika = tika
      @sparql_connection_pool = sparql_connection_pool
      @search_index = search_index

      @configuration = search_configuration
      @number_of_threads = search_configuration[:number_of_threads]
      @batch_size = search_configuration[:batch_size]
      @max_batches = search_configuration[:max_batches]
      @attachment_path_base = search_configuration[:attachment_path_base]

      type_def = @configuration[:type_definitions][search_index.type_name]
      if type_def.is_composite_index?
        @index_definitions = type_def.composite_types
      else
        @index_definitions = [type_def]
      end
    end

    # Index the documents for the configured type definition in batches.
    #
    # The properties are queried from the triplestore using the SPARQL connection pool
    # which is configured with the appropriate mu-auth-allowed-groups.
    #
    # If a document fails to index, a warning will be logged, but the indexing continues.
    # The other documents in the batch will still be indexed.
    def build
      # Note: @index_definitions will only contain multiple elements in case of a composite type.
      @index_definitions.each do |type_def|
        @logger.info("INDEXING") { "Building index of type #{type_def["type"]}" }
        rdf_types = type_def.related_rdf_types
        number_of_documents = count_documents(rdf_types)
        @logger.info("INDEXING") { "Found #{number_of_documents} documents to index of type #{rdf_types.join(',')} with allowed groups #{@search_index.allowed_groups}" }
        batches =
          if @max_batches and @max_batches != 0
            [@max_batches, number_of_documents/@batch_size].min
          else
            number_of_documents/@batch_size
          end
        batches = batches + 1
        @logger.info("INDEXING") { "Number of batches: #{batches}" }

        Parallel.each( 1..batches, in_threads: @number_of_threads ) do |i|
          batch_start_time = Time.now
          @logger.info("INDEXING") { "Indexing batch #{i}/#{batches}" }
          failed_documents = []

          @sparql_connection_pool.with_authorization(@search_index.allowed_groups) do |sparql_client|
            document_builder = MuSearch::DocumentBuilder.new(
              tika: @tika,
              sparql_client: sparql_client,
              attachment_path_base: @attachment_path_base,
              logger: @logger
            )
            document_uris = get_documents_for_batch(rdf_types, i)
            document_uris.each do |document_uri|
              @logger.debug("INDEXING") { "Indexing document #{document_uri} in batch #{i}" }
              document = document_builder.fetch_document_to_index(
                uri: document_uri,
                properties: type_def["properties"])
              @elasticsearch.insert_document @search_index.name, document_uri, document
            rescue StandardError => e
              failed_documents << document_uri
              @logger.warn("INDEXING") { "Failed to index document #{document_uri} in batch #{i}" }
              @logger.warn { e.full_message }
            end
          end
          @logger.info("INDEXING") { "Processed batch #{i}/#{batches} in #{(Time.now - batch_start_time).round} seconds." }
          if failed_documents.length > 0
            @logger.warn("INDEXING") { "#{failed_documents.length} documents failed to index in batch #{i}." }
            @logger.debug("INDEXING") { "Failed documents: #{failed_documents}" }
          end
        end
      end
    end


    private
    def count_documents(types)
      @sparql_connection_pool.with_authorization(@search_index.allowed_groups) do |client|
        type_string = types.map{ |type| ::SinatraTemplate::Utils.sparql_escape_uri(type) }.join(',')
        query = "SELECT (COUNT(?doc) as ?count) WHERE { ?doc a ?type. filter(?type in(#{type_string})) }"
        result = client.query(query)
        documents_count = result.first["count"].to_i
        documents_count
      end
    end

    def get_documents_for_batch(types, batch_i)
      offset = (batch_i - 1) * @batch_size
      type_string = types.map{ |type| ::SinatraTemplate::Utils.sparql_escape_uri(type) }.join(',')
      @sparql_connection_pool.with_authorization(@search_index.allowed_groups) do |client|
        query = "SELECT DISTINCT ?doc WHERE { ?doc a ?type. filter(?type in(#{type_string}))  } LIMIT #{@batch_size} OFFSET #{offset}"
        result = client.query(query)
        document_uris = result.map { |r| r[:doc].to_s }
        @logger.debug("INDEXING") { "Selected documents for batch #{batch_i}: #{document_uris}" }
        document_uris
      end
    end
  end
end
