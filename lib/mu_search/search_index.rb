module MuSearch
  class SearchIndex
    attr_reader :uri, :name, :type_name, :allowed_groups, :used_groups, :mutex
    attr_accessor :is_eager_index, :status
    def initialize(uri:, name:, type_name:, is_eager_index:, allowed_groups:, used_groups:)
      @uri = uri
      @name = name
      @is_eager_index = is_eager_index
      @type_name = type_name
      @allowed_groups = allowed_groups
      @used_groups = used_groups

      @status = :valid  # possible values: :valid, :invalid, :updating
      @mutex = Mutex.new
    end

    def eager_index?
      @is_eager_index
    end
  end
end
