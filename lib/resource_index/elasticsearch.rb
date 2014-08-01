module ResourceIndex
  module Elasticsearch
    def concept_count(hash, opts = {})
      expand = opts[:expand] == true ? true : false
      return expand ? es_count(hash, [:direct, :ancestors]) : es_count(hash)
    end

    def concept_docs(hash, opts = {})
      expand = opts[:expand] == true ? true : false
      size   = opts[:size] || 10
      return expand ? es_doc(hash, size, [:direct, :ancestors]) : es_doc(hash, size)
    end

    private

    def es_doc(hash, size, types = [:direct])
      (RI.es.search index: self.acronym, body: query(hash, types, size))["hits"]["hits"]
    end

    def es_count(hash, types = [:direct])
      count = RI.es.count index: self.acronym, body: query(hash, types)
      count["count"] || 0
    end

    def query(hash, types = [:direct], size = nil)
      query = {
        query: {
          nested: {
            path: "annotations",
            query: {
              multi_match: {
                query: hash,
                fields: types.map {|type| "annotations.#{type}"}
              }
            }
          }
        }
      }
      query[:size] = size if size
      query
    end
  end
end
