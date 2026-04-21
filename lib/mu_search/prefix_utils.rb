module MuSearch
  module PrefixUtils
    def self.expand_prefix(uri, prefixes)
      return uri unless uri.is_a?(String)

      uri
        .split("|")
        .map do |part|
          prefixes.each do |prefix, base_uri|
            if part.start_with?("#{prefix}:")
              part = part.sub("#{prefix}:", base_uri)
              break
            end
          end
          part
        end
        .join("|")
    end
  end
end