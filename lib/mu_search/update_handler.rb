require 'set'
require 'yaml/store'

module MuSearch
  ##
  # the update handler is a service that executes updates or deletes on indexes.
  # updates are collected in a FIFO queue and executed after a certain wait interval has expired
  # NOTE: recommend use is a specific implementations:
  #  - InvalidatingUpdateHandler
  #  - AutomaticUpdateHandler
  # You can also use this class, but the handler needs to be provided as a block, e.g.
  # UpdateHandler.new(...) do |subject, index_names, type|
  # end
  class UpdateHandler
    ##
    # default interval to wait before applying changes
    DEFAULT_WAIT_INTERVAL_MINUTES = 1
    ##
    # default number of threads to use for handling updates
    DEFAULT_NUMBER_OF_THREADS = 2

    ##
    # creates an update handler
    def initialize(logger:, index_manager:, search_configuration:, &block)
      @logger = logger
      @index_manager = index_manager
      @mutex = Mutex.new
      define_method(:handler, block) if block_given?
      @number_of_threads =
        if search_configuration[:number_of_threads] > 0
          search_configuration[:number_of_threads]
        else
          DEFAULT_NUMBER_OF_THREADS
        end

      wait_interval =
        if search_configuration[:update_wait_interval_minutes].nil?
          DEFAULT_WAIT_INTERVAL_MINUTES
        else
          search_configuration[:update_wait_interval_minutes]
        end
      @min_wait_time = wait_interval * 60 / 86400.0

      # FIFO queue of outstanding update actions, max. 1 per subject
      @queue = []
      # In memory cache of index types to update per subject
      @subject_map = Hash.new { |hash, key| hash[key] = Set.new }

      restore_queue_and_setup_persistence
      setup_runners

      @logger.info("UPDATE HANDLER") { "Update handler configured with #{@number_of_threads} threads and wait time of #{wait_interval} minutes" }
    end

    ##
    # add an action to the queue
    # type should be either :update or :delete
    def add(subject, index_type, type)
      @mutex.synchronize do
        was_empty = @queue.empty?
        # Add subject to queue if an update for the same subject hasn't been scheduled before
        if !@subject_map.has_key? subject
          @logger.debug("UPDATE HANDLER") { "Add update for subject <#{subject}> to queue" }
          @queue << { timestamp: DateTime.now, subject: subject, type: type }
        else
          @logger.debug("UPDATE HANDLER") { "Update for subject <#{subject}> already scheduled" }
        end
        @subject_map[subject].add(index_type)
        # Only signal when the queue was empty, as runners are already
        # waiting with a timeout when there are items in the queue
        @condition.signal if was_empty
      end
    end

    ##
    # add an update to be handled
    # wrapper for add
    def add_update(subject, index_type)
      add(subject, index_type, :update)
    end

    ##
    # add a delete to be handled
    # wrapper for add
    def add_delete(subject, index_type)
      add(subject, index_type, :delete)
    end

    private

    # Setup a runner per thread to handle updates.
    # Threads block on @condition until an item is ready to be processed.
    def setup_runners
      @condition = ConditionVariable.new
      @runners = (0...@number_of_threads).map do |i|
        Thread.new(abort_on_exception: true) do
          @logger.debug("UPDATE HANDLER") { "Runner #{i} ready for duty" }
          loop do
            change = subject = index_types = type = nil
            begin
              @mutex.synchronize do
                until @queue.length > 0 && (DateTime.now - @queue[0][:timestamp]) > @min_wait_time
                  if @queue.length > 0
                    # Wait until the oldest item is ready
                    remaining_seconds = (@min_wait_time - (DateTime.now - @queue[0][:timestamp])) * 86400.0
                    @condition.wait(@mutex, [remaining_seconds, 0.1].max)
                  else
                    # Queue empty, wait until signaled
                    @condition.wait(@mutex)
                  end
                end
                change = @queue.shift
                subject = change[:subject]
                type = change[:type]
                index_types = @subject_map.delete(subject)
              end

              if @queue.length > 500 && @queue.length % 100 == 0
                @logger.warn("UPDATE HANDLER") { "Large number of updates (#{@queue.length}) in queue" }
              end
              @logger.debug("UPDATE HANDLER") { "Handling update of #{subject}" }
              handler(subject, index_types, type)
            rescue StandardError => e
              @logger.error("UPDATE HANDLER") { "Update of subject <#{subject}> failed" }
              @logger.error("UPDATE HANDLER") { e.full_message }
            end
          end
        end
      end
    end

    # Initializes the update queue and ensures the queue is persisted on disk at regular intervals
    def restore_queue_and_setup_persistence
      if File.exist?("/config/update-handler.store")
        @logger.warn("UPDATE HANDLER") do
          "Found update-handler.store in /config. " \
          "Since v0.13.0 the store must be located at /data/update-handler.store. " \
          "Please move the file and update your volume mounts. " \
          "See the CHANGELOG for migration instructions."
        end
      end

      @store = YAML::Store.new("/data/update-handler.store", true)
      @store.transaction do
        @queue = @store.fetch("queue", [])
        @subject_map = @subject_map.merge(@store.fetch("index", {}))
        @logger.info("UPDATE HANDLER") { "Restored update queue (length: #{@queue.length})" }
      end

      @persister = Thread.new(abort_on_exception: true) do
        loop do
          sleep 300
          @mutex.synchronize do
            @logger.info("UPDATE HANDLER") { "Persisting update queue to disk (length: #{@queue.length})" }
            begin
              @store.transaction do
                @store["queue"] = @queue
                @store["index"] = @subject_map
              end
            rescue StandardError => e
              @logger.error("UPDATE HANDLER") { "Failed to persist update queue to disk" }
              @logger.error("UPDATE HANDLER") { e.full_message }
            end
          end
        end
      end
    end
  end
end
