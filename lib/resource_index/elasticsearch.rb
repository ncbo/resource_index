require_relative 'document'

module ResourceIndex
  module Elasticsearch
    def concept_count(hash, opts = {})
      es_concept_count(hash, opts).map {|doc| RI::Document.from_elasticsearch(doc, self)}
    end

    def concept_docs(hash, opts = {})
      es_concept_docs(hash, opts).map {|doc| RI::Document.from_elasticsearch(doc, self)}
    end

    def es_concept_count(hash, opts = {})
      es_count(hash, opts)
    end

    def es_concept_docs(hash, opts = {})
      es_doc(hash, opts)
    end

    private

    def es_doc(hash, opts = {})
      opts[:size] ||= 10
      opts[:from] ||= 0
      (RI.es.search index: self.acronym, body: query(hash, opts))["hits"]["hits"]
    end

    def es_count(hash, opts = {})
      count = RI.es.count index: self.acronym, body: query(hash, opts)
      count["count"] || 0
    end

    def query(hashes, opts = {})
      hashes = hashes.is_a?(Array) ? hashes : [hashes]
      expand = opts[:expand] == true ? true : false
      types  = expand ? [:direct, :ancestors] : [:direct]
      bool   = opts[:bool] || :must # passing :should finds documents with any class, :must finds documents with all classes
      size   = opts[:size]
      from   = opts[:from]

      bool_query = {
        bool => hashes.map {|hash| {:bool => {:should => types.map {|t| {match: {"annotations.#{t}" => hash}} } } } }
      }

      query = {
        query: {
          nested: {
            path: "annotations",
            query: {
              bool: bool_query
            }
          }
        }
      }
      query[:size] = size if size
      query[:from] = from if from
      query
    end
  end
end
