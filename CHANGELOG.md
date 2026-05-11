# Changelog
## Unreleased
:boom: **Breaking**
- Change mount point of `update-handler.store` to `/data` instead of `/config`
- Change mount point of files to `/share` instead of `/data`

To upgrade execute the following steps in your project folder
```bash
mkdir -p ./data/search
mv ./config/search/update-handler.store ./data/search
```

Next, update the mount points of the `search` service
```yaml
services:
  search:
    image: semtech/mu-search
    volumes:
      - ./config/search:/config
      - ./data/search:/data
      - ./data/files:/share # in case you index files
      - ./data/tika/cache:/cache # in case you index files
```


## v0.12.0
**Features**
- Support for Elasticsearch 9.2.0 (via elasticsearch gem 9.x)
- Backward compatible with Elasticsearch 7.x backends
- Support for URI prefixes in search configuration (`prefixes` config option)
- Batched VALUES pipeline for delta handling — groups triples by query shape and executes batched SPARQL queries instead of per-triple queries
- Configurable `delta_batch_size` for controlling the number of triples per VALUES query batch (default: 100)
- Raw DSL endpoint now validates that request body does not contain an `index` property (prevents cross-index data access)

**Fixes**
- Fixed empty language strings being sent to Elasticsearch (rejects empty string language tags)
- Fixed undefined variable in log statement (#104)
- Fixed bug on non-persisted (dynamic) indexes
- Fixed document updates by replacing documents on upsert instead of update
- Fixed subject check in delta handler

**Changes**
- Upgraded elasticsearch gem from 7.17 to 9.2
- Error namespace changed from `Elasticsearch::Transport` to `Elastic::Transport`
- Renamed internal class `Elastic` to `ElasticWrapper` to avoid naming conflicts
- Replaced sleep-polling loops with `ConditionVariable` (update handler) and `Concurrent::Event` (search index wait)
- Replaced `Array` + `Mutex` delta queue with thread-safe `Thread::Queue`
- Extracted JSON:API formatting, authorization utils, and query validation into separate modules
- Refactored property handling to consistently use `PropertyDefinition` objects

## v0.11.0
**Features**
- Connection pooling with configurable connection pool size for requests to
  - Tika
  - Elasticsearch
  - triplestore
- mu-cli script to manage indexes
- Make max yaml size configurable via MAX_YAML_SIZE env var
- Experimental support for ignoring and dynamic allowed groups

## v0.10.0
**Fixes**
- [#92](https://github.com/mu-semtech/mu-search/issues/92) fix restoring update-handler if file exceeds 3MB

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
