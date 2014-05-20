require 'elasticsearch'
require 'ostruct'
require 'json'
require_relative 'class'
require_relative 'elastic_search_index'

module RI::Population

  def self.config(opts = {})
    @settings = s = OpenStruct.new
    s.annotator_redis_host = opts[:annotator_redis_host] || "localhost"
    s.annotator_redis_port = opts[:annotator_redis_port] || 6379
    s.population_threads   = opts[:population_threads] || 2
    s.es_url               = opts[:es_url] || "http://localhost:9200"
    s.es_client            = Elasticsearch::Client.new(url: @es_url)
    nil
  end

  def self.settings
    @settings
  end

  def self.new_index

  end

  private

  def self.es_mapping(res)
    raise ArgumentError, "Please provide a resource" unless res.is_a?(RI::Resource)
    properties_json = Hash[res.fields.values.map {|f| [f.name.to_sym, {type: :string}] }]
    # ES_MAPPING.sub("%doc_type%", res.acronym).sub("%properties_json%", JSON.dump(properties_json))
    {
      mappings: {
        res.acronym => {
          :"_source" => {
            includes: res.fields.values.map {|f| f.name.to_sym},
            excludes: :annotations
          },
          properties: properties_json.merge(ANNOTATIONS_MAPPING)
        }
      }
    }
  end

ANNOTATIONS_MAPPING = {
  annotations: {
      type: :nested,
      include_in_all: false,
      properties: {
          direct: {type: :long, store: false, include_in_all: false},
          ancestors: {type: :long, store: false, include_in_all: false},
          count: {type: :long, store: false, include_in_all: false}
      }
  }
}

<<-EOS
{
    "mappings": {
        "%doc_type%": {
            "_source" : {
              "includes": ["title", "abstract"],
              "excludes": ["annotations"]
            },
            "properties": {
                %properties_json%,
                "annotations": {
                    "type": "nested",
                    "include_in_all": false,
                    "properties": {
                        "direct": {"type": "long", "store": false, "include_in_all": false},
                        "ancestors": {"type": "long", "store": false, "include_in_all": false},
                        "count": {"type": "long", "store": false, "include_in_all": false}
                    }
                }
            }
        }
    }
}
EOS
end
