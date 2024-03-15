require_relative './search_index'

module MuSearch
  ###
  # The IndexManager keeps track of indexes and their state in:
  # - an in-memory cache @indexes, grouped per type
  # - Elasticsearch, using index.name as identifier
  # - triplestore
  ###
  class IndexManager
    attr_reader :indexes

    def initialize(logger:, elasticsearch:, tika:, sparql_connection_pool:, search_configuration:)
      @logger = logger
      @elasticsearch = elasticsearch
      @tika = tika
      @sparql_connection_pool = sparql_connection_pool
      @master_mutex = Mutex.new
      @configuration = search_configuration
      @indexes = {} # indexes per type

      initialize_indexes
    end

    # Fetches an array of indexes for the given type and allowed/used groups
    # Ensures all indexes exists and are up-to-date when the function returns
    # If no type is passed, indexes for all types are fetched
    # If no allowed_groups are passed, all indexes are fetched regardless of access rights
    #   - type_name: type to find index for
    #   - allowed_groups: allowed groups to find index for (array of {group, variables}-objects)
    #   - force_update: whether the index needs to be updated only when it's marked as invalid or not
    #
    # Returns an array of indexes that match the allowed_groups when combined
    # Returns an empty array if no index is found
    def fetch_indexes(type_name, allowed_groups, force_update: false)
      indexes_to_update = []
      type_names = type_name.nil? ? @indexes.keys : [type_name]

      @master_mutex.synchronize do
        type_names.each do |type_name|
          if allowed_groups
            additive_indexes = ensure_index_combination_for_groups type_name, allowed_groups
            indexes_to_update += additive_indexes
          elsif @indexes[type_name] # fetch all indexes, regardless of access rights
            @indexes[type_name].each do |_, index|
              @logger.debug("INDEX MGMT") { "Fetched index for type '#{type_name}' and allowed_groups #{index.allowed_groups}: #{index.name}" }
              indexes_to_update << index
            end
          end
        end

        indexes_to_update.each do |index|
          index.status = :invalid if force_update
        end
      end
      indexes_to_update.each do |index|
        update_index index
      end
      if indexes_to_update.any? { |index| index.status == :invalid }
          @logger.warn("INDEX MGMT") { "Not all indexes are up-to-date. Search results may be incomplete." }
      end
      indexes_to_update
    end

    # Invalidate the indexes for the given type and allowed groups
    # If no type is passed, indexes for all types are invalidated
    # If no allowed_groups are passed, all indexes are invalidated regardless of access rights
    # - type_name: name of the index type to invalidate all indexes for
    # - allowed_groups: allowed groups to invalidate indexes for (array of {group, variables}-objects)
    #
    # Returns the list of indexes that are invalidated
    #
    # TODO correctly handle composite indexes
    def invalidate_indexes(type_name, allowed_groups)
      indexes_to_invalidate = []
      type_names = type_name.nil? ? @indexes.keys : [type_name]

      @master_mutex.synchronize do
        type_names.each do |type_name|
          if allowed_groups
            index = find_single_index_for_groups type_name, allowed_groups
            indexes_to_invalidate << index unless index.nil?
          elsif @indexes[type_name] # invalidate all indexes, regardless of access rights
            @indexes[type_name].each do |_, index|
              indexes_to_invalidate << index
            end
          end
        end

        @logger.info("INDEX MGMT") do
          type_s = type_name.nil? ? "all types" : "type '#{type_name}'"
          allowed_groups_s = allowed_groups.nil? ? "all groups" : "allowed_groups #{allowed_groups}"
          index_names_s = indexes_to_invalidate.map(&:name).join(", ")
          "Found #{indexes_to_invalidate.length} indexes to invalidate for #{type_s} and #{allowed_groups_s}: #{index_names_s}"
        end

        indexes_to_invalidate.each do |index|
          @logger.debug("INDEX MGMT") { "Mark index #{index.name} as invalid" }
          index.mutex.synchronize { index.status = :invalid }
        end
      end

      indexes_to_invalidate
    end

    # Remove the indexes for the given type and allowed groups
    # If no type is passed, indexes for all types are removed
    # If no allowed_groups are passed, all indexes are removed regardless of access rights
    # - type name: name of the index type to remove all indexes for
    # - allowed_groups: allowed groups to remove indexes for (array of {group, variables}-objects)
    #
    # Returns the list of indexes that are removed
    #
    # TODO correctly handle composite indexes
    def remove_indexes(type_name, allowed_groups)
      indexes_to_remove = []
      @master_mutex.synchronize do
        if allowed_groups
          index = find_single_index_for_groups type_name, allowed_groups
          indexes_to_remove << index unless index.nil?
        elsif @indexes[type_name] # remove all indexes, regardless of access rights
          @indexes[type_name].each do |_, index|
            indexes_to_remove << index
          end
        end
      end
      @logger.info("INDEX MGMT") do
        allowed_groups_s = allowed_groups.nil? ? "all groups" : "allowed_groups #{allowed_groups}"
        index_names_s = indexes_to_remove.map(&:name).join(", ")
        "Found #{indexes_to_remove.length} indexes to remove for #{type_name} and #{allowed_groups_s}: #{index_names_s}"
      end

      indexes_to_remove.each do |index|
        @logger.debug("INDEX MGMT") { "Remove index #{index.name}" }
        index.mutex.synchronize do
          remove_index(index)
          index.status = :deleted
        end
      end

      indexes_to_remove
    end

    private

    # Initialize indexes based on the search configuration
    # Ensures all configured eager indexes exist
    # and removes indexes found in the triplestore if index peristentce is disabled
    def initialize_indexes
      if @configuration[:persist_indexes]
        @logger.info("INDEX MGMT") { "Loading persisted indexes from the triplestore" }
        @configuration[:type_definitions].keys.each do |type_name|
          @indexes[type_name] = get_indexes_from_triplestore_by_type type_name
        end
      else
        @logger.info("INDEX MGMT") { "Removing indexes as they're configured not to be persisted. Set the 'persist_indexes' flag to 'true' to enable index persistence (recommended in production environment)." }
        remove_persisted_indexes
      end

      @logger.info("INDEX MGMT") { "Start initializing all configured eager indexing groups..." }
      @master_mutex.synchronize do
        total = @configuration[:eager_indexing_groups].length * @configuration[:type_definitions].keys.length
        count = 0
        @configuration[:eager_indexing_groups].each do |allowed_groups|
          @configuration[:type_definitions].keys.each do |type_name|
            count = count + 1
            index = ensure_index(type_name, allowed_groups, [], true)
            @logger.info("INDEX MGMT") { "(#{count}/#{total}) Eager index #{index.name} created for type '#{index.type_name}' and allowed_groups #{allowed_groups}. Current status: #{index.status}." }
            if index.status == :invalid
              @logger.info("INDEX MGMT") { "Eager index #{index.name} not up-to-date. Start reindexing documents." }
              index_documents index
              index.status = :valid
            end
          end
        end
        @logger.info("INDEX MGMT") { "Completed initialization of #{total} eager indexes" }
      end
    end

    # Find a single index for the given type that exactly matches the given allowed/used groups
    #   - type_name: type to find index for
    #   - allowed_groups: allowed groups to find index for (array of {group, variables}-objects)
    #   - used_groups: used groups to find index for (array of {group, variables}-objects)
    # Returns nil if no index is found
    #
    # TODO take used_groups into account when they are supported by mu-authorization
    def find_single_index_for_groups(type_name, allowed_groups, used_groups = [])
      @logger.debug("INDEX MGMT") { "Trying to find single matching index in cache for type '#{type_name}', allowed_groups #{allowed_groups} and used_groups #{used_groups}" }
      group_key = serialize_authorization_groups allowed_groups
      index = @indexes.dig(type_name, group_key)
      index
    end

    # Find matching index combination for the given type and allowed/used groups
    #   - type_name: type to find index for
    #   - allowed_groups: allowed groups to find index for (array of {group, variables}-objects)
    #   - used_groups: used groups to find index for (array of {group, variables}-objects)
    # If no index combination is found, a single index is created for the given set of allowed_groups
    #
    # TODO take used_groups into account when they are supported by mu-authorization
    def ensure_index_combination_for_groups(type_name, allowed_groups, used_groups = [])
      @logger.debug("INDEX MGMT") { "Trying to combine indexes in cache for type '#{type_name}' to match allowed_groups #{allowed_groups} and used_groups #{used_groups}" }

      indexes = @indexes[type_name].values.find_all(&:eager_index?)
      @logger.debug("INDEX MGMT") { "Currently known indexes for type '#{type_name}': #{indexes.map(&:allowed_groups).to_json}" }
      # Find all indexes with allowed_groups that are a subset of the given allowed_groups
      matching_indexes = indexes.find_all do |idx|
        idx.allowed_groups.all? do |idx_allowed_group|
          allowed_groups.include? idx_allowed_group
        end
      end

      # Only keep indexes which are not a subset of/equal to another index in the list
      minimal_matching_indexes = matching_indexes.reject do |idx|
        matching_indexes.find do |other_idx|
          idx.allowed_groups.all? { |group| other_idx.allowed_groups.include? group } and other_idx.allowed_groups.count > idx.allowed_groups.count # we are a strict subset, not the same set
        end
      end

      # Verify whether allowed_groups match is complete.
      # I.e. the combination of allowed groups of the matching indexes cover the given allowed_groups
      is_complete_match = allowed_groups.all? do |allowed_group|
        minimal_matching_indexes.any? do |idx|
          idx.allowed_groups.include? allowed_group
        end
      end

      if is_complete_match
        @logger.debug("INDEX MGMT") do
          "Fetched #{minimal_matching_indexes.length} additive indexes for type '#{type_name}' that fully match allowed_groups #{allowed_groups}: #{minimal_matching_indexes.map(&:name).join(', ')}\nMatching allowed groups of the indexes: #{minimal_matching_indexes.map(&:allowed_groups).to_json}"
        end
        minimal_matching_indexes
      else
        @logger.info("INDEX MGMT") do
          "Unable to find an index combination for type '#{type_name}' to fully match allowed_groups #{allowed_groups}. Going to create a new index.}"
        end
        index = ensure_index type_name, allowed_groups, used_groups
        [index]
      end
    end

    # Ensure index exists in the triplestore, in Elasticsearch and
    # in the in-memory indexes cache of the IndexManager
    #
    # Returns the index with status :valid or :invalid depending
    # whether the index already exists in Elasticsearch
    def ensure_index(type_name, allowed_groups, used_groups = [], is_eager_index = false)
      sorted_allowed_groups = sort_authorization_groups allowed_groups
      sorted_used_groups = sort_authorization_groups used_groups
      index_name = generate_index_name type_name, sorted_allowed_groups, sorted_used_groups

      # Ensure index exists in triplestore
      index_uri = find_index_in_triplestore_by_name index_name
      unless index_uri
        @logger.debug("INDEX MGMT") { "Create index #{index_name} in triplestore for type '#{type_name}', allowed_groups #{allowed_groups} and used_groups #{used_groups}" }
        index_uri = create_index_in_triplestore type_name, index_name, sorted_allowed_groups, sorted_used_groups
      end

      # Ensure index exists in the IndexManager
      index = find_single_index_for_groups type_name, allowed_groups, used_groups
      if index
        index.is_eager_index = is_eager_index
      else
        @logger.debug("INDEX MGMT") { "Add index #{index_name} to cache for type '#{type_name}', allowed_groups #{allowed_groups} and used_groups #{used_groups}" }
        index = MuSearch::SearchIndex.new(
          uri: index_uri,
          name: index_name,
          type_name: type_name,
          allowed_groups: sorted_allowed_groups,
          used_groups: sorted_used_groups,
          is_eager_index: is_eager_index)
        @indexes[type_name] = {} unless @indexes.has_key? type_name
        group_key = serialize_authorization_groups sorted_allowed_groups
        @indexes[type_name][group_key] = index
      end

      # Ensure index exists in Elasticsearch
      unless @elasticsearch.index_exists? index_name
        @logger.debug("INDEX MGMT") { "Creating index #{index_name} in Elasticsearch for type '#{type_name}', allowed_groups #{allowed_groups} and used_groups #{used_groups}" }
        index.status = :invalid
        type_definition = @configuration[:type_definitions][type_name]
        if type_definition
          mappings = type_definition["mappings"] || {}
          mappings["properties"] = {} if mappings["properties"].nil?
          # uuid must be configured as keyword to be able to collapse results
          mappings["properties"]["uuid"] = { type: "keyword" }
          mappings["properties"]["uri"] = { type: "keyword" }
          # TODO deep merge custom and default settings
          settings = type_definition["settings"] || @configuration[:default_index_settings] || {}
          @elasticsearch.create_index index_name, mappings, settings
        else
          raise "No type definition found in search config for type '#{type_name}'. Unable to create Elasticsearch index."
        end
      end
      index
    end

    # Updates an existing index if it's current state is invalid
    # I.e. clear all documents in the Elasticsearch index
    # and index the documents again.
    # The Elasticsearch index is never completely removed.
    #   - index: SearchIndex to update
    # Returns the index.
    def update_index(index)
      if index.status == :invalid
        index.mutex.synchronize do
          @logger.info("INDEX MGMT") { "Updating index #{index.name}" }
          index.status = :updating
          begin
            @elasticsearch.clear_index index.name
            index_documents index
            @elasticsearch.refresh_index index.name
            index.status = :valid
            @logger.info("INDEX MGMT") { "Index #{index.name} is up-to-date" }
          rescue StandardError => e
            index.status = :invalid
            @logger.error("INDEX MGMT") { "Failed to update index #{index.name}." }
            @logger.error("INDEX MGMT") { e.full_message }
          end
        end
      end
      index
    end

    # Indexes documents in the given SearchIndex.
    # I.e. index documents for a specific type in the given Elasticsearch index
    # taking the authorization groups into account. Documents are indexed in batches.
    #   - index: SearchIndex to index documents in
    def index_documents(index)
      search_configuration = @configuration.select do |key|
        [:number_of_threads, :batch_size, :max_batches,
         :attachment_path_base, :type_definitions].include? key
      end
      builder = MuSearch::IndexBuilder.new(
        logger: @logger,
        elasticsearch: @elasticsearch,
        tika: @tika,
        sparql_connection_pool: @sparql_connection_pool,
        search_index: index,
        search_configuration: search_configuration)
      builder.build
    end

    # Removes the index from the triplestore, Elasticsearch
    # and the in-memory indexes cache of the IndexManager.
    def remove_index(index)
      @indexes.delete(index)
      remove_index_by_name(index.name)
    end

    # Removes the index from the triplestore and Elasticsearch
    # Does not yield an error if index doesn't exist
    def remove_index_by_name(index_name)
      @logger.debug("INDEX MGMT") { "Removing index #{index_name} from triplestore" }
      remove_index_from_triplestore index_name

      if @elasticsearch.index_exists? index_name
        @logger.debug("INDEX MGMT") { "Removing index #{index_name} from Elasticsearch" }
        @elasticsearch.delete_index index_name
      end
    end

    # Removes all persisted indexes from the triplestore as well as from Elasticsearch
    #
    # NOTE this method does not check the current search configuration.
    #      It only removes indexes found in the triplestore and removes those.
    def remove_persisted_indexes
      result = @sparql_connection_pool.sudo_query <<SPARQL
SELECT ?name WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?index a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex>;
               <http://mu.semte.ch/vocabularies/authorization/indexName> ?name
    }
  }
SPARQL
      index_names = result.map(&:name)
      index_names.each do |index_name|
        remove_index_by_name index_name
        @logger.info("INDEX MGMT") { "Remove persisted index #{index_name} in triplestore and Elasticsearch" }
      end
    end

    # Stores a new index in the triplestore
    #
    #   - type_name: Type of the objects stored in the index
    #   - index_name: Unique name of the index (also used as id in Elasticsearch)
    #   - allowed_groups: allowed groups of the index (array of {group, variables}-objects)
    #   - used_groups: used groups of the index (array of {group, variables}-objects)
    #
    # TODO cleanup internal model used for storing indexes in triplestore
    def create_index_in_triplestore(type_name, index_name, allowed_groups, used_groups)
      uuid = Mu::generate_uuid
      uri = "http://mu.semte.ch/authorization/elasticsearch/indexes/#{uuid}" # TODO update base URI

      def groups_term(groups)
        groups.map { |g| Mu::sparql_escape_string g.to_json }.join(",")
      end

      allowed_group_statement = allowed_groups.empty? ? "" : "search:hasAllowedGroup #{groups_term(allowed_groups)} ; "
      used_group_statement = used_groups.empty? ? "" : "search:hasUsedGroup #{groups_term(used_groups)} ; "

      query_result = @sparql_connection_pool.sudo_update <<SPARQL
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX search: <http://mu.semte.ch/vocabularies/authorization/>
  INSERT DATA {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> a search:ElasticsearchIndex ;
               mu:uuid "#{uuid}" ;
               search:objectType "#{type_name}" ;
               #{allowed_group_statement}
               #{used_group_statement}
               search:indexName "#{index_name}" .
    }
  }
SPARQL
      uri
    end

    # Removes the index with given name from the triplestore
    #
    #   - index_name: name of the index to remove
    def remove_index_from_triplestore(index_name)
      @sparql_connection_pool.sudo_update <<SPARQL
DELETE {
  GRAPH <http://mu.semte.ch/authorization> {
    ?s ?p ?o .
  }
}
WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?s a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex> ;
           <http://mu.semte.ch/vocabularies/authorization/indexName> #{Mu::sparql_escape_string index_name} ;
           ?p ?o .
    }
}
SPARQL
    end

    # Find index by name in the triplestore
    # Returns nil if none is found
    def find_index_in_triplestore_by_name(index_name)
      result = @sparql_connection_pool.sudo_query <<SPARQL
SELECT ?index WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?index a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex> ;
               <http://mu.semte.ch/vocabularies/authorization/indexName> #{Mu::sparql_escape_string index_name} .
    }
  } LIMIT 1
SPARQL
      result.map(&:index).first
    end

    # Gets indexes for the given type name from the triplestore
    #
    # - type_name: name of the index type as configured in the search config
    #
    # Note: there may be multiple indexes for one type.
    #       One per (combination of) allowed groups
    def get_indexes_from_triplestore_by_type(type_name)
      indexes = {}

      query_result = @sparql_connection_pool.sudo_query <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?index a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex> ;
                 <http://mu.semte.ch/vocabularies/authorization/objectType> "#{type_name}" ;
                 <http://mu.semte.ch/vocabularies/authorization/indexName> ?index_name .
    }
  }
SPARQL

      query_result.each do |result|
        uri = result["index"].to_s
        index_name = result["index_name"].to_s

        allowed_groups_result = @sparql_connection_pool.sudo_query <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> <http://mu.semte.ch/vocabularies/authorization/hasAllowedGroup> ?group
    }
  }
SPARQL
        allowed_groups = allowed_groups_result.map { |g| JSON.parse g["group"].to_s }

        used_groups_result = @sparql_connection_pool.sudo_query <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> <http://mu.semte.ch/vocabularies/authorization/hasUsedGroup> ?group
    }
  }
SPARQL
        used_groups = used_groups_result.map { |g| JSON.parse g["group"].to_s }

        group_key = serialize_authorization_groups allowed_groups

        indexes[group_key] = MuSearch::SearchIndex.new(
          uri: uri,
          name: index_name,
          type_name: type_name,
          is_eager_index: false, # will be overwritten later on initialization of eager indexes
          allowed_groups: allowed_groups,
          used_groups: used_groups)
      end

      indexes
    end

    # Generate a unique name for an index based on the given type and allowed/used groups
    def generate_index_name(type_name, sorted_allowed_groups, sorted_used_groups)
      groups = sorted_allowed_groups.map do |group|
        # order keys of each group object alphabetically to ensure unique json serialization
        Hash[group.sort_by { |key, _| key }].to_json
      end
      Digest::MD5.hexdigest (type_name + "-" + groups.join("-"))
    end
  end
end
