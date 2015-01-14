module ResourceIndex::Population
  class AnnotationCounter

    def initialize(sql_host, sql_user, sql_pass, es_hosts, es_port = 9200)
      RI.config(username: sql_user, password: sql_pass, host: sql_host, es_hosts: es_hosts, es_port: es_port)
      @es = ResourceIndex.es.dup
    end

    def count_and_store
      resources = RI::Resource.populated
      resources.each do |res|
        puts res.acronym
        @es.index index: :counts, type: :annotation, id: res.acronym, body: count(res)
        puts count(res)
      end
    end

    def count(res)
      index = res.current_index_id
      direct_results = @es.search({
        index: index,
        search_type: :count,
        body: query('direct')
      })
      ancestor_results = @es.search({
        index: index,
        search_type: :count,
        body: query('ancestors')
      })
      return {
        direct: direct_results["aggregations"]["annotations"]["count"]["value"],
        ancestors: ancestor_results["aggregations"]["annotations"]["count"]["value"]
      }
    end

    def query(type)
      return {
          aggregations: {
              annotations: {
                  aggs: {
                      count: {
                          value_count: {
                              field: "annotations.#{type}"
                          }
                      }
                  },
                  nested: {
                      path: "annotations"
                  }
              }
          }
      }
    end


  end
end