require 'set'
require 'pp'

module MuSearch
  ##
  # the delta handler is a service that parses deltas and triggers
  # the necessary updates via the (index) update handler.
  # Assumes that it is safe to remove objects for which the type was removed
  # updates documents for deltas that match the configured property paths
  # NOTE: in theory the handler has a pretty good idea what has changed
  #       it may be possible to have finer grained updates on es documents than we currently have
  class DeltaHandler
    ##
    # creates a delta handler
    #
    # raises an error if an invalid search config is provided
    def initialize(logger:, sparql_connection_pool:, search_configuration:, update_handler:)
      @logger = logger
      @sparql_connection_pool = sparql_connection_pool
      @type_definitions = search_configuration[:type_definitions]
      @update_handler = update_handler
      # FIFO queue of deltas
      @queue = []
      @mutex = Mutex.new
      setup_runner
    end

    # Setup a runner per thread to handle updates
    def setup_runner
      @runner = Thread.new(abort_on_exception: true) do
        @logger.info("DELTA") { "Runner ready for duty" }
        loop do
          triple = delta = resource_configs = nil
          begin
            @mutex.synchronize do
              if @queue.length > 0
                delta = @queue.shift
              end
            end
            if delta
              triples = delta[:triples]
              resource_configs = delta[:resource_configs]
              handle_queue_entry(triples, resource_configs)
            end
          rescue StandardError => e
            @logger.error("DELTA") { "Failed processing delta #{delta.pretty_inspect}" }
            @logger.error("DELTA") { e.full_message }
          end
          sleep 0.05
        end
      end
    end

    ##
    # Parses the given delta and adds it to the queue to trigger the update of affected documents
    # Assumes delta format v0.0.1
    def handle_deltas(deltas)
      @logger.debug("DELTA") { "Received delta update #{deltas.pretty_inspect}" }
      if deltas.is_a?(Array)
        @logger.debug("DELTA") { "Delta contains #{deltas.length} changesets" }
        triples = []
        deltas.each do |changeset|
          triples += changeset["inserts"].map { |triple| triple.merge({ "is_addition" => true }) }
          triples += changeset["deletes"].map { |triple| triple.merge({ "is_addition" => false }) }
        end
        find_config_and_queue_delta(triples)
      else
        @logger.error("DELTA") { "Received delta does not seem to be in v0.0.1 format. Mu-search currently only supports delta format v0.0.1 " }
        @logger.error("DELTA") { deltas.pretty_inspect }
      end
    end


    private

    ##
    # Find the affected indexes for a given changeset and add it to the queue
    #
    def find_config_and_queue_delta(triples)
      @logger.debug("DELTA") { "Handling delta: #{triples.inspect}" }
      search_configs = Set.new
      triples.each do |triple|
        search_configs += applicable_index_configurations_for_triple(triple)
        type_names = search_configs.map(&:name)
        @logger.debug("DELTA") { "Delta affects #{type_names.length} search indexes: #{type_names.join(', ')}" }
      end

      @mutex.synchronize do
        @queue << { triples: triples, resource_configs: search_configs }
      end
    end

    ##
    # queues necessary update of indexes based on received delta
    #
    def handle_queue_entry(triples, resource_configs)
      resource_configs.each do |config|
        subjects = Set.new
        triples.each do |triple|
          subjects += find_root_subjects_for_triple(triple, config, triple["is_addition"])
        end
        if subjects.size
          type_name = config.name
          @logger.debug("DELTA") { "Found #{subjects.length} subjects for resource config '#{type_name}' that needs to be updated." }
          subjects.each { |subject| @update_handler.add_update(subject, type_name) }
        end
      end
    end

    ##
    # Find index configs that are impacted by the given triple,
    # i.e. the object is an rdf:Class that is configured as search index
    #      or the predicate is included in one of the property (paths) of a search index.
    # Returns a set of impacted search configs.
    # Each config contains keys :type_name, :rdf_types, :rdf_properties
    def applicable_index_configurations_for_triple(triple)
      predicate = triple["predicate"]["value"]
      if predicate == RDF.type.to_s
        rdf_type = triple["object"]["value"]
        @type_definitions.select { |name, definition| definition.matches_type?(rdf_type) }.values
      else
        @type_definitions.select { |name, definition| definition.matches_property?(predicate) }.values
      end
    end

    ##
    # Finds the root subjects related to the given triple for a given search config
    # - triple: changed triple received in delta message
    # - config: search config for a type affected by the changed triple
    # - is_addition: whether the triple is inserted or deleted
    #
    # Returns an array of subject URIs as strings.
    # Returns an empty array if no subjects are found.
    def find_root_subjects_for_triple(triple, config, is_addition = true)
      # NOTE: current logic assumes rdf:type is never part of the property path
      if triple["predicate"]["value"] == RDF.type.to_s
        [triple["subject"]["value"]]
      else
        find_subjects_for_property(triple, config, is_addition)
      end
    end

    ##
    # Finds the subjects related to the given triple
    # with the configured rdf_type via the configured property path
    # - triple: changed triple received in delta message
    # - config: search config for a type affected by the changed triple
    # - is_addition: whether the triple is inserted or deleted
    #
    # Returns an array of subject URIs as strings.
    # Returns an empty array if no subjects are found.
    # TODO: this needs some form of cache
    def find_subjects_for_property(triple, config, is_addition)
      predicate = triple["predicate"]["value"]
      object_type = triple["object"]["type"]
      subjects = []
      matching_property_paths = config.full_property_paths_for(predicate)
      matching_property_paths.each do |path|
        path.each_with_index do |property, i|
          if predicate_matches_property?(predicate, property)
            if (i < path.length - 1) && !is_inverse?(property) && (object_type != "uri")
              # we are not at the end of the path and the object is a literal
              @logger.debug("DELTA") { "Discarding path because object is not a URI, but #{object_type}" }
            else
              subjects_for_property = query_for_subjects_to_triple(
                triple,
                config.related_rdf_types,
                path,
                i,
                is_inverse?(property),
                is_addition)
              subjects.concat(subjects_for_property)
            end
          end
        end
      end
      subjects
    end

    # checks if a predicate or its inverse equals the property
    def predicate_matches_property?(predicate, property)
      [predicate, "^#{predicate}"].include?(property)
    end

    # check if the property is inverse
    def is_inverse?(property)
      property.start_with? "^"
    end

    ##
    # Queries the triplestore to find subjects related to the given triple
    # with the configured rdf_type via the configured property path
    # - triple: changed triple received in delta message
    # - config: search config for a type affected by the changed triple
    # - i: index number of the triple predicate in the property path of the search config
    # - is_inverse: whether the triple's predicate is included as inverse predicate in the property path of the search config
    # - is_addition: whether the triple is inserted or deleted
    #
    # Returns an array of subject URIs as strings.
    # Returns an empty array if no subjects are found.
    def query_for_subjects_to_triple(triple, rdf_types, path, i, is_inverse, is_addition)
      property_path_to_target = path.take(i) # path from start to the triple, excluding the triple itself
      property_path_from_target = path.drop(i + 1) # path from the triple until the end
      # escaping values for usage in the SPARQL query
      path_to_target_term = MuSearch::SPARQL::make_predicate_string(property_path_to_target)
      path_from_target_term = MuSearch::SPARQL::make_predicate_string(property_path_from_target)

      subject_value = triple["subject"]["value"]
      predicate_value = triple["predicate"]["value"]
      triple_object = triple["object"]
      object_value = triple_object["value"]
      object_type = triple_object["type"]
      object_datatype = triple_object["datatype"]
      object_language = triple_object["xml:lang"]

      if object_type == "uri"
        object_term = Mu::sparql_escape_uri(object_value)
      elsif object_language
        object_term = %(#{object_value.sparql_escape}@#{object_language})
      elsif object_datatype
        object_term = %(#{object_value.sparql_escape}^^#{Mu::sparql_escape_uri(object_datatype)})
      else
        object_term = %(#{object_value.sparql_escape})
      end

      rdf_type_terms = rdf_types.map{ |rdf_type| Mu::sparql_escape_uri(rdf_type)}

      # Based on the direction of the predicate, determine the target to which the property_path leads
      target_subject_term = is_inverse ? Mu::sparql_escape_uri(object_value) : Mu::sparql_escape_uri(subject_value)
      target_object_term = is_inverse ? Mu::sparql_escape_uri(subject_value) : object_term

      # Build SPARQL query that tries to match the full path in the triplestore
      sparql_query = "SELECT DISTINCT ?s WHERE {\n"
      sparql_query += "\t ?s a ?type. \n"
      sparql_query += "FILTER(?type IN (#{rdf_type_terms.join(',')})) . \n"
      if property_path_to_target.length == 0
        # Triple is at the root. We only need to check if it has the correct rdf_type
        sparql_query += "\t VALUES ?s { #{target_subject_term} } . \n"
      else
        # Check path from root to the triple
        sparql_query += "\t ?s #{path_to_target_term} #{target_subject_term} . \n"
      end
      if is_addition
        # Check the delta triple itself
        sparql_query += "\t #{Mu::sparql_escape_uri(subject_value)} #{Mu::sparql_escape_uri(predicate_value)} #{object_term} . \n"
        # Check path from the triple to the end
        if property_path_from_target.length > 0
          sparql_query += "\t #{target_object_term} #{path_from_target_term} ?foo. \n"
        end
      # else:
      #   in case of a deletion, we cannot check the remainder of the path
      #   as the triple no longer exists in the triplestore
      end
      sparql_query += "}"

      @sparql_connection_pool.sudo_query(sparql_query).map { |result| result["s"].to_s }
    end
  end
end
