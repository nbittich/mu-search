require_relative '../lib/mu_search/index_definition.rb'
require 'json'

RSpec.describe MuSearch::CompositeSubIndexDefinition do
  it "initializes" do
    id = MuSearch::CompositeSubIndexDefinition.new(
      name: "test",
      rdf_type: "http://test.com/Test",
      properties: {}
    )
  end
end

JSON_CONFIG = JSON.parse(<<EOF
[
  {
      "type": "mandatory",
      "on_path": "mandatories",
      "rdf_type": ["http://data.vlaanderen.be/ns/mandaat#Mandataris", "http://data.lblod.info/vocabularies/erediensten/EredienstMandataris"],
      "properties": {
        "given_name": [
          "http://data.vlaanderen.be/ns/mandaat#isBestuurlijkeAliasVan",
          "http://xmlns.com/foaf/0.1/givenName"
        ],
        "family_name": [
          "http://data.vlaanderen.be/ns/mandaat#isBestuurlijkeAliasVan",
          "http://xmlns.com/foaf/0.1/familyName"
        ],
        "first_name_used": [
          "http://data.vlaanderen.be/ns/mandaat#isBestuurlijkeAliasVan",
          "https://data.vlaanderen.be/ns/persoon#gebruikteVoornaam"
        ]
     }
  },

    {
      "type": "agent",
      "on_path": "agents",
      "rdf_type": "http://data.lblod.info/vocabularies/leidinggevenden/Functionaris",
      "properties": {
        "given_name": [
          "http://data.vlaanderen.be/ns/mandaat#isBestuurlijkeAliasVan",
          "http://xmlns.com/foaf/0.1/givenName"
        ],
        "family_name": [
          "http://data.vlaanderen.be/ns/mandaat#isBestuurlijkeAliasVan",
          "http://xmlns.com/foaf/0.1/familyName"
        ],
        "first_name_used": [
          "http://data.vlaanderen.be/ns/mandaat#isBestuurlijkeAliasVan",
          "https://data.vlaanderen.be/ns/persoon#gebruikteVoornaam"
        ],
        "end_date": "http://data.vlaanderen.be/ns/mandaat#einde",
        "organization_id": [
          "http://www.w3.org/ns/org#holds",
          "^http://data.lblod.info/vocabularies/leidinggevenden/heeftBestuursfunctie",
          "https://data.vlaanderen.be/ns/generiek#isTijdspecialisatieVan",
          "http://data.vlaanderen.be/ns/besluit#bestuurt",
          "http://mu.semte.ch/vocabularies/core/uuid"
        ],
        "organization_province": [
          "http://www.w3.org/ns/org#holds",
          "^http://data.lblod.info/vocabularies/leidinggevenden/heeftBestuursfunctie",
          "http://www.w3.org/ns/locn#adminUnitL2"
        ]
      }
  },
  {
      "type": "person",
      "composite_types": [
        "agent",
        "mandatory"
      ],
      "on_path": "people",
      "properties": [
        {
          "name": "given_name",
          "mappings": {
            "agent": "first_name_used",
            "mandatory": "given_name"
          }
        },
        {
          "name": "family_name"
        },
        {
          "name": "first_name_used"
        }]
   }
]
EOF
                        )
RSpec.describe MuSearch::IndexDefinition do
  context "running from_json_config on a config with subindexes" do
    subject { MuSearch::IndexDefinition.from_json_config(JSON_CONFIG) }

    it "should return 3 indexes" do
      expect(subject.length).to eq(3)
    end

    it "should contain 1 composite index" do
      expect(subject.select{ |name, index| index.is_composite_index?}.length).to eq(1)
    end

    context "the returned composite index" do
      it "should have 2 subindexes" do
        composite_index = subject.select{ |name, index| index.is_composite_index?}[0][1]
        expect(composite_index.composite_types.length).to eq(2)
      end

      it "should match the specified property" do
        composite_index = subject.select{ |name, index| index.is_composite_index?}[0][1]
        expect(composite_index.matches_property?("http://data.vlaanderen.be/ns/mandaat#isBestuurlijkeAliasVan")).to be true
      end

      it "should not match a non specified property" do
        composite_index = subject.select{ |name, index| index.is_composite_index?}[0][1]
        expect(composite_index.matches_property?("http://data.europa.eu/eli/ontology#title")).to be false
      end

      it "should match the specified types" do
        composite_index = subject.select{ |name, index| index.is_composite_index?}[0][1]
        expect(composite_index.matches_type?("http://data.vlaanderen.be/ns/mandaat#Mandataris")).to be true
      end

      context "its subindexes" do
        it "should return the correct properties" do
          composite_index = subject.select{ |name, index| index.is_composite_index?}[0][1]
          sub_indexes = composite_index.composite_types
          expect(sub_indexes[0]).to be_a MuSearch::CompositeSubIndexDefinition
          expect(sub_indexes[0].properties).to be_a Hash
          expect(sub_indexes[0].properties).to have_key("given_name")
          expect(sub_indexes[0].properties["given_name"]).to be_a Array
        end
      end
    end
  end
end
