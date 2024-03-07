# Monkeypatching SPARQL::Client to correctly parse language tag for CONSTRUCT queries
module SPARQL
  class Client
    ##
    # @param  [Hash{String => String}] value
    # @return [RDF::Value]
    # @see    https://www.w3.org/TR/sparql11-results-json/#select-encode-terms
    # @see    https://www.w3.org/TR/rdf-sparql-json-res/#variable-binding-results
    def self.parse_json_value(value, nodes = {})
      case value['type'].to_sym
        when :bnode
          nodes[id = value['value']] ||= RDF::Node.new(id)
        when :uri
          RDF::URI.new(value['value'])
        when :literal
          # Monkey patch: add support for 'lang' instead of only 'xml:lang'
          # SELECT queries return the language in 'xml:lang',
          # but CONSTRUCT queries return them in 'lang'
          language = value['xml:lang'] || value['lang']
          RDF::Literal.new(value['value'], datatype: value['datatype'], language: language)
        when :'typed-literal'
          RDF::Literal.new(value['value'], datatype: value['datatype'])
        when :triple
          s = parse_json_value(value['value']['subject'], nodes)
          p = parse_json_value(value['value']['predicate'], nodes)
          o = parse_json_value(value['value']['object'], nodes)
          RDF::Statement(s, p, o)
        else nil
      end
    end
  end
end
