module ResourceIndex
  module Elasticsearch
    def concept_count(hash, opts = {})
      expand = opts[:expand] == true ? true : false
      return expand ? ancestor_count(hash) : direct_count(hash)
    end

    def concept_docs(hash, opts = {})
      expand = opts[:expand] == true ? true : false
      size   = opts[:size] || 10
      return expand ? ancestor_doc(hash, size) : direct_doc(hash, size)
    end

    private

    def direct_doc(hash, size)
      (RI.es.search index: self.acronym, body: query(hash, :direct, size))["hits"]["hits"]
    end

    def ancestor_doc(hash, size)
      (RI.es.search index: self.acronym, body: query(hash, :ancestors, size))["hits"]["hits"]
    end

    def direct_count(hash)
      count = RI.es.count index: self.acronym, body: query(hash) rescue binding.pry
      count["count"] || 0
    end

    def ancestor_count(hash)
      count = RI.es.count index: self.acronym, body: query(hash, :ancestors)
      count["count"] || 0
    end

    def query(hash, type = :direct, size = nil)
      query = {
        query: {
          nested: {
            path: "annotations",
            query: {
              match: {
                :"annotations.#{type}" => hash
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
