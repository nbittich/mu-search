# Changelog
## v0.10.0-beta.5
**Fixes**
- Fix uuid ensurance for composite type indexes

## v0.10.0-beta.4
**Features**
- Add query param to get exact count in search results
- Add filter flags :id: and :uri: to filter by id and URI
- Support wait interval less than 1 min in update handler
- ARM64 builds

**Changes**
- Determine documents to be updated in Elasticsearch per delta message instead of per triple

## v0.10.0-beta.3
- change: Use 1 construct query instead of 1 query per property to fetch properties for a document

## v0.10.0-beta.2
- fix: Use tagged base image of mu-jruby-template
- fix: re-add support for deletes in delta-handling
- fix: proper support for nested objects in delta handling (note still does not take the nested type into account)

## v0.10.0-beta.1
- change: put delta handling and lookups in a separate thread

## v0.9.0
- fix: Base image bumped to fix runtime warning (see issue [25](https://github.com/mu-semtech/mu-ruby-template/pull/25) on the mu-ruby-template)

## v0.9.0-beta.7
**Fixes**
-  fix escaping of values when handling delta's

## v0.9.0-beta.6
**Fixes**
- bump base image to properly listen to signals

## v0.9.0-beta.5
**Fixes**
- the admin endpoint to delete indexes should now work correctly ( https://github.com/mu-semtech/mu-search/issues/59 )
- search should no longer be blocked when a new index is created ( https://github.com/mu-semtech/mu-search/issues/42 )

## v0.9.0-beta.4
**Features**
-  changes how additive indexes work. search indexes defined in `eager_indexing_groups` can now partially match the user's allowed groups. Indexes will be combined at search time to fully match the incoming allowed groups. If no combination can be found a single index matching the user's allowed groups will be created. This also means additive indexes are no longer opt in, but given proper eager index definitions this should be fully backwards compatible.
- experimental support for indexing language strings

## v0.9.0-beta.3
**Fixes**
- match delta predicates in both directions
## v0.9.0-beta.2
**Fixes**
- fix handling deletion of a resource

**Features**
- basic highlighting support

## v0.9.0-beta.1
**Features**
- better support for composite indexes
- experimental support for having multiple rdf types in one (non composite) index

**Fixes**
- misc fixes for delta handling

## v0.8.0
## v0.8.0-beta.4
**Features**
- Allow to specify wildcards and per-field boosts in fields parameter
- multi match support for fuzzy filter
- Basic config validation on startup
- include URI of nested objects in the document

**Fixes**
- No longer throw an error in the delta handler when composite types are used (composite types are still not handled though)
- improved, but far from perfect composite type support


## v0.8.0-beta.3
**Fixes**
- Using connection pool for all SPARQL queries

## v0.8.0-beta.2
**Fixes**
- Error handling in case a file is not found for indexing
- Allow all permutations of lt(e),gt(e) in search query params

## v0.8.0-beta.1
**Features**
- Extracting file content using external Tika service
- Configurable log levels per scope
- Improved error logging
- Add documentation on eager indexing groups and update queues

**Fixes**
- Taking authorization headers into account on index management endpoints
- Indexing of boolean values
- Allow dashes and underscores in search property names
- Ensure same index name independently of order of auth group keys

## v0.7.0
**Features**
- Add `:has:` and `:has-no:` filter flags to filter on any/no value
- Support multiple fields for the `:phrase:` and `:phrase_prefix:` filter flags
- Make request URL and headers length configurable
- Improve documentation structure and examples

**Fixes**
- Indexing of nested objects

## v0.6.3
**Fixes**
- refactored indexing operations
- use connection pool for sparql connections
