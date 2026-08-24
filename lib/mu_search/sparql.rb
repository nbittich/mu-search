require 'connection_pool'

module MuSearch
  module SPARQL
    class ClientWrapper
      def initialize(sparql_client:, options:)
        @sparql_client = sparql_client
        @options = options
      end

      def query(query_string)
        Mu::log.debug("SPARQL") { "Executing query with #{@options.inspect}\n#{query_string}" }
        @sparql_client.query query_string, **@options
      end

      def update(query_string)
        Mu::log.debug("SPARQL") { "Executing update with #{@options.inspect}\n#{query_string}" }
        @sparql_client.update query_string, **@options
      end
    end

    class ConnectionPool
      @instance = nil

      def self.setup(size: 4)
        @instance = ::ConnectionPool.new(size: size, timeout: 3) do
          ::SPARQL::Client.new(ENV['MU_SPARQL_ENDPOINT'])
        end
        Mu::log.info("SETUP") { "Setup SPARQL connection pool with #{@instance.size} connections." }
      end

      def self.instance
        if @instance
          @instance
        else
          raise "SPARQL connection pool not yet initialized. Please call MuSearch::SPARQL::ConnectionPool.setup() first"
        end
      end

      def self.up?
        begin
          self.sudo_query "ASK { ?s ?p ?o }", 1
        rescue StandardError => e
          false
        end
      end

      ##
      # perform an update with access to all data
      def self.sudo_query(query_string, retries = 6)
        begin
          self.with_sudo do |sudo_client|
            sudo_client.query query_string
          end
        rescue StandardError => e
          next_retries = retries - 1
          if next_retries == 0
            raise e
          else
            Mu::log.warn("SPARQL") { "Could not execute sudo query (attempt #{6 - next_retries}): #{query_string}" }
            timeout = (6 - next_retries)**2
            sleep timeout
            sudo_query query_string, next_retries
          end
        end
      end

      ##
      # perform an update with access to all data
      def self.sudo_update(query_string, retries = 6)
        begin
          self.with_sudo do |sudo_client|
            sudo_client.update query_string
          end
        rescue StandardError => e
          next_retries = retries - 1
          if next_retries == 0
            raise e
          else
            Mu::log.warn("SPARQL") { "Could not execute sudo query (attempt #{6 - next_retries}): #{query_string}" }
            timeout = (6 - next_retries)**2
            sleep timeout
            sudo_update query_string, next_retries
          end
        end
      end

      ##
      # provides a client from the connection pool with the given access rights
      def self.with_authorization(allowed_groups, &block)
        sparql_options = {}

        if allowed_groups && allowed_groups.length > 0
          allowed_groups_s = allowed_groups.select { |group| group }.to_json
          sparql_options = { headers: { 'mu-auth-allowed-groups': allowed_groups_s } }
        end

        self.with_options sparql_options, &block
      end

      private

      ##
      # provides a client from the connection pool with sudo access rights
      def self.with_sudo(&block)
        self.with_options({ headers: { 'mu-auth-sudo': 'true' } }, &block)
      end

      def self.with_options(sparql_options)
        self.instance.with do |sparql_client|
          Mu::log.debug("SPARQL") { "Claimed SPARQL connection from pool. #{self.instance.available}/#{self.instance.size} connections are still available." }
          client_wrapper = ClientWrapper.new(sparql_client: sparql_client, options: sparql_options)
          yield client_wrapper
        end
      end
    end

    # Converts the given predicate to an escaped predicate used in a SPARQL query.
    #
    # The string may start with a ^ sign to indicate inverse.
    # If that exists, we need to interpolate the URI.
    #
    #   - predicate: Predicate to be escaped.
    def self.predicate_string_term(predicate)
      if predicate.start_with? "^"
        "^#{Mu::sparql_escape_uri(predicate.slice(1, predicate.length))}"
      else
        Mu::sparql_escape_uri(predicate)
      end
    end

    # Converts the SPARQL predicate definition from the config into a
    # triple path.
    #
    # The configuration in the configuration file may contain an inverse
    # (using ^) and/or a list (using the array notation).  These need to
    # be converted into query paths so we can correctly fetch the
    # contents.
    #
    #   - predicate: Predicate definition as supplied in the config file.
    #     Either a string or an array.
    #
    # TODO: I believe the construction with the query paths leads to
    # incorrect invalidation when delta's arrive. Perhaps we should store
    # the relevant URIs in the stored document so we can invalidate it
    # correctly when new content arrives.
    def self.make_predicate_string(predicate)
      if predicate.is_a? String
        predicate_string_term(predicate)
      else
        predicate.map { |pred| predicate_string_term pred }.join("/")
      end
    end
  end
end
