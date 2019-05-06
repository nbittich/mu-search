require 'parallel'

def is_multiple_type? type_definition
  type_definition["composite_types"].is_a?(Array)
end


def multiple_type_expand_subtypes types, properties
  types.map do |type|
    source_type_def = settings.type_definitions[type]
    rdf_type = source_type_def["rdf_type"]

    { 
      "type" => type,
      "rdf_type" => rdf_type,
      "properties" => Hash[
        properties.map do |property|
          property_name = property["name"]
          mapped_name = 
            if property["mappings"]
              property["mappings"][type] || property_name
            else 
              property_name
            end
          [property_name, source_type_def["properties"][mapped_name]]
        end
      ]
    }
  end
end


def index_documents client, type, index, allowed_groups = nil
  count_list = [] # for reporting

  type_def = settings.type_definitions[type]

  if is_multiple_type?(type_def)
    type_defs = multiple_type_expand_subtypes type_def["composite_types"], type_def["properties"]
  else
    type_defs = [type_def]
  end

  type_defs.each do |type_def|
    rdf_type = type_def["rdf_type"]

    count = count_documents rdf_type, allowed_groups
    count_list.push({type: type_def["type"], count: count})
    properties = type_def["properties"]

    log.info "Indexing #{count} documents of type: #{type_def["type"]}"

    batches =
      if settings.max_batches and settings.max_batches != 0
        [settings.max_batches, count/settings.batch_size].min
      else
        count/settings.batch_size
      end

    (0..batches).each do |i|
      log.info "indexing batch #{i} of #{count/settings.batch_size}"
      offset = i*settings.batch_size
      data = []
      attachments = {}
      q = <<SPARQL
    SELECT DISTINCT ?id WHERE {
      ?doc a <#{rdf_type}>;
           <http://mu.semte.ch/vocabularies/core/uuid> ?id
    } LIMIT #{settings.batch_size} OFFSET #{offset}
SPARQL

    query_result =
      if allowed_groups
        authorized_query q, allowed_groups
      else
        request_authorized_query q
      end

      Parallel.each( query_result, in_threads: 16 ) do |result|
        uuid = result[:id].to_s
        document, attachment_pipeline = fetch_document_to_index uuid: uuid, properties: properties, allowed_groups: allowed_groups

        if attachment_pipeline
          begin
            client.upload_attachment index, uuid, attachment_pipeline, document
          rescue
            log.info "Failed to upload attachment for document uuid: #{uuid}"
          end
        else
          data.push({ index: { _id: uuid } })
          data.push document
        end
      end

      client.bulk_update_document index, data unless data.empty?
    end
  end

  { index: index, document_types: count_list }
end

