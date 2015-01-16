require 'time'

# Simple median enumerable monkeypatch
module Enumerable
  def median(&block)
    return map(&block).median if block_given?

    sorted = sort
    count  = sorted.size
    i      = count / 2

    return nil if count == 0
    if count % 2 == 1
      sorted[i]
    else
      ( sorted[i-1] + sorted[i] ) / 2.0
    end.to_f
  end
end

##
# AnnotationCounter goes through each resource and counts the number of annotations.
# This is done for both direct and expanded (hierarchy) annotations.
# We skip resources with more than 150k records because ES can't hold the results in memory.
#
# This code should be invoked in a separate script, it doesn't get run automatically and
# it is not a part of the population process. It stores the results of the count into
# Elasticsearch in an index called `counts`. There is a method on the top-level ResourceIndex
# module that retrieves the counts using the most recent entry.
#
#
# Example of how to invoke the script
#
# require 'ncbo_resource_index'
# ac = RI::Population::AnnotationCounter.new("sql_document_host", "user", "pass", ["es_node_1", "es_node_2"])
# ac.count_and_store
#
# Retrieve counts by:
# RI.config()
# RI.counts()
module ResourceIndex::Population
  class AnnotationCounter

    def initialize(opts = {})
      RI.config(opts) unless RI.settings
      @es = ResourceIndex.es.dup
    end

    def count_and_store
      resources = RI::Resource.populated
      counts = { raw: {} }
      direct_counts = []
      ancestor_counts = []
      doc_counts = []
      resources.each do |res|
        puts "Skipping #{res.acronym}" if res.count > 150_000
        next if res.count > 150_000
        puts res.acronym
        annotation_count = count(res)
        doc_counts << res.count
        direct_counts << annotation_count[:direct]
        ancestor_counts << annotation_count[:ancestors]
        counts[:raw][res.acronym] = annotation_count
      end

      direct_median = direct_counts.median / doc_counts.median
      ancestor_median = ancestor_counts.median / doc_counts.median

      direct_count = 0
      resources.each {|r| direct_count += r.count * direct_median}
      ancestor_count = 0
      resources.each {|r| ancestor_count += r.count * ancestor_median}

      time = Time.now.utc.iso8601
      counts[:total] = { direct: direct_count.to_i, ancestors: ancestor_count.to_i }
      counts[:time] = time

      @es.index index: :counts, type: :annotation, id: "counts_#{time}", body: counts
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