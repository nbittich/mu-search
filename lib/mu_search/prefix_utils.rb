module MuSearch
  module PrefixUtils
    def self.expand_prefix(uri, prefixes)
      return uri unless uri.is_a?(String)

      prefixes.each do |prefix, base_uri|
        if uri.start_with?("#{prefix}:")
          return uri.sub("#{prefix}:", base_uri)
        end
      end
      uri
    end
  end
end
