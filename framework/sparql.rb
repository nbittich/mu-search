def query_with_headers query, headers
  sparql_client.query query, { headers: headers }
end


def authorized_query query, allowed_groups
  allowed_groups_object = allowed_groups.map { |group| { value: group } }.to_json
  sparql_client.query query, { headers: { MU_AUTH_ALLOWED_GROUPS: allowed_groups_object } }
end


# Shouldn't be necessary: ruby template should pass headers on
def request_authorized_query query
  sparql_client.query query, { headers: { MU_AUTH_ALLOWED_GROUPS: request.env["HTTP_MU_AUTH_ALLOWED_GROUPS"] } }
end


def direct_query q
  settings.db.query q
end


def count_documents rdf_type, allowed_groups = nil
  sparql_query =  <<SPARQL
      SELECT (COUNT(?doc) AS ?count) WHERE {
        ?doc a <#{rdf_type}>
      }
SPARQL

  query_result =
    if allowed_groups
      authorized_query sparql_query, allowed_groups
    else
      request_authorized_query sparql_query
    end

  query_result.first["count"].to_i
end


# memoized
def is_type s, rdf_type
  @subject_types ||= {}
  if @subject_types.has_key? [s, rdf_type]
    @subject_types[s, rdf_type]
  else
    direct_query "ASK WHERE { <#{s}> a <#{rdf_type}> }"
  end
end


# memoized
def is_authorized s, rdf_type, allowed_groups
  @authorizations ||= {}
  if @authorizations.has_key? [s, rdf_type, allowed_groups]
    @authorizations[s, rdf_type, allowed_groups]
  else

    authorized_query "ASK WHERE { <#{s}> a <#{rdf_type}> }", allowed_groups
  end
end


def get_uuid s
  query_result = direct_query <<SPARQL
SELECT ?uuid WHERE {
   <#{s}> <http://mu.semte.ch/vocabularies/core/uuid> ?uuid 
}
SPARQL
  uuid = query_result.first["uuid"]
end


def make_property_query uuid, uri, properties
  select_variables_s = ""
  property_predicates = []

  id_line = 
    if uuid
      "?doc <http://mu.semte.ch/vocabularies/core/uuid> \"#{uuid}\". "
    else
      " "
    end

  s = uuid ? "?doc" : "<#{uri}>"

  properties.each do |key, predicate|
    select_variables_s += " ?#{key} " 
    predicate_s = predicate.is_a?(String) ? predicate : predicate.join("/")
    property_predicates.push " OPTIONAL { #{s} <#{predicate}> ?#{key} } "
  end

  property_predicates_s = property_predicates.join(" ")

  <<SPARQL
    SELECT #{select_variables_s} WHERE { 
     #{id_line}
     #{property_predicates_s}     
    }
SPARQL
end


def fetch_document_to_index uuid: nil, uri: nil, properties: nil, allowed_groups: nil
  query_result =
    if allowed_groups
      authorized_query make_property_query(uuid, uri, properties), allowed_groups
    else
      request_authorized_query make_property_query(uuid, uri, properties)
    end
  
  result = query_result.first

  document = Hash[
    properties.collect do |key, val|
      [key, result[key].to_s] # !! get right types!
    end
  ]            
end




def sparql_up
  begin 
    direct_query "ASK { ?s ?p ?o }"
  rescue
    false
  end
end