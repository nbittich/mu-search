---
Stage: Draft
Start Date: 17-10-2022
Release Date: Unreleased
RFC PR: https://github.com/mu-semtech/mu-search/pull/57
---

# multilang support in musearch

## Summary
In RDF literals of the xsd:langString type can carry a language, for example `"english string"@en`. The goal of this RFC is to define how musearch should support language tagged literals so that this information (the language tag) is not lost in the conversion to a elasticsearch document. The minimum requirement of this RFCS is to support one value per label, but the solution should cater for later inclusion of multiple labels in a specific language.


## Motivation
In some projects we have data that has entities that have a label in several languages, it should be possible to only include the relevant labels in search results. This means the search api needs to be able to provide a way for the consumer to specify the preferred language and a way to filter out irrelevant languages.


## Detailed design
### storage in elasticsearch
There are two common ways of handling multi language search: a separate index per language or one index with all languages. This RFC assumes using one index, but there are good reasons for using multiple indexes as explained in [multilingual search](https://www.algolia.com/doc/guides/managing-results/optimize-search-results/handling-natural-languages-nlp/how-to/multilingual-search/). 

When storing multiple languages in one index there are three common ways of storing language values in a json document:
#### language containers
```json
{
"label": {
    "en": "The Queen",
    "de": [ "Die Königin", "Ihre Majestät" ]
  }
}
```

#### expanded form
```json
{
"label": [
    {"@value": "The Queen", "@language": "en"},
    {"@value": "Die Königin", "@language": "de"},
    {"@value": "Ihre Majestät", "@language": "de"}
  ]
}
```

#### post or prefixed fields
```json
{
"label_en": "The Queen",
"label_de": [ "Die Königin", "Ihre Majestät" ]
}
```


Of these, only language containers and postfixed fields allow a user to define custom tokenizers and analysers for specific languages. So the expanded form should not be considered a candidate for storage. 

Of the two remaining options the language container seems the most robust solution, by nesting the languages we make parsing and finding the relative languages a lot easier and avoid the chance (however small) of conflicting with another defined property.

Having chosen the storage format, we must decide how we define our language containers:

#### literals without a language tag
This needs to be researched, currently no idea how this is typically handled. A solution could be to provide "none" as a container for literals without a language tag. Very open to better ways of expressing this in the document.
```json
{
"label": {
    "default": "The Queen",
  }
}
```

#### do we merge language subtypes?
In RDF any language tag is allowed, though it is recommended to follow some standard. We should consider whether we do some cleanup of these or if we keep the full language type as specified in the store. For this RFC it seems recommended to keep it simple and retain the exact value in the store. We should be aware that data cleanup may be necessary for a proper index so that we don't end up with the following

```json
{
"label": {
   "de": "Die Königin",
   "deu": "Ihre Majestät",
   "de-AT": "Ihrer Majestät"
   }
}
```

#### backwards compatibility
Musearch could choose to change the storage of all string literals to a language container, but it seems this would be breaking even if we would hide this in the api itself. Afterall the mappings field contains specific elasticsearch settings that need to match elasticsearch internals. It would be hard for us to translate those without knowing the data that is to be ingested. As such it's probably required to allow a user to specify the type of a property.


### config change: support property types
A typical type/index definition looks like this currently
```json
  {
      "type": "cases",
      "on_path": "cases",
      "rdf_type": "http://mu.semte.ch/vocabularies/ext/Case",
      "properties": {
        "title": "http://purl.org/dc/terms/title",
        "author": "http://purl.org/dc/terms/author",
        "related": [
          "^http://purl.org/dc/terms/related",
          "http://purl.org/dc/terms/title"
        ],
        "theme": [
          "http://purl.org/dc/terms/theme",
          "http://www.w3.org/2004/02/skos/core#prefLabel"
        ],
        "data": {
          "via": [
            "http://purl.org/dc/terms/hasPart",
            "^http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource"
          ],
          "attachment_pipeline": "attachment"
        }
      },
```

In essence musearch is not aware of the type of data being indexed, rather this is offloaded to elasticsearch. For language tagged literals it should be possible to specify the fact that the language tag needs to be stored. 

mu-search already supports objects instead of a plain path for properties for both nested objects and attachment pipelines (as shown above).
This RFC suggests expanding the object with an optional `type` to specify a specific type. Currently only three types would be offered: `simple`, `nested` and `language-string`. `simple` would be the default if type is not defined, nested applies to any property that is a hash and has a `properties` key. An example:

```json
  {
      "type": "cases",
      "on_path": "cases",
      "rdf_type": "http://mu.semte.ch/vocabularies/ext/Case",
      "properties": {
        "title": {
          "via": "http://purl.org/dc/terms/title",
          "type": "language-string"
        },
        "author": "http://purl.org/dc/terms/author",
        "related": {
          "via": [
            "^http://purl.org/dc/terms/related",
            "http://purl.org/dc/terms/title"
          ],
          "type": "language-string"
        },
        "theme": [
          "http://purl.org/dc/terms/theme",
          "http://www.w3.org/2004/02/skos/core#prefLabel"
        ],
        "data": {
          "via": [
            "http://purl.org/dc/terms/hasPart",
            "^http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource"
          ],
          "attachment_pipeline": "attachment"
        }
      },
```

### extensions
- specifying the default language is probably something that should be added later on, but considered out of scope for this RFC.
- identifying the language (instead of relying on the language tag) is [possible with elastic](https://www.elastic.co/blog/multilingual-search-using-language-identification-in-elasticsearch), but does not match this RFCs use case.

### querying
The simplest option seems to be to treat the language fields as regular fields and require consumers/clients to specify the fields directly. e.g. `/cases/search?filter[label.de]=Majestät`. This provides quite a bit of flexibility to the client. The main advantage is that searching in multiple languages and specifying importance per language becomes easy and explicit. 

Other options are requiring the user to specify the language in a global parameter. This can simplify the query a bit, since that doesn't require the client to specify the language(s) on all relevant fields and also can be used to limit the response to only the requested languages (this last bit is also achieved when the fields are made explicit).

## references
- https://www.elastic.co/blog/multilingual-search-using-language-identification-in-elasticsearch
- https://www.algolia.com/doc/guides/managing-results/optimize-search-results/handling-natural-languages-nlp/how-to/multilingual-search/
