require 'goo'
require 'ruby-xxhash'

module RI::Population
  class Class
    attr_accessor :id, :ont, :ont_acronym, :sub

    def initialize(id, ont, ont_acronym, sub = nil)
      @id = id; @ont = ont; @ont_acronym = ont_acronym; @sub = sub
    end

    def retrieve_ancestors(acronym, submission_id)
      ids = retrieve_hierarchy_ids(:ancestors, submission_id)
      if ids.length == 0
        return []
      end
      ids.lazy.select { |x| !x["owl#Thing"] }.map {|id| XXhash.xxh32(acronym + id, 112233)}.force
    end

    def xxhash
      @xxhash ||= XXhash.xxh32(self.ont_acronym + self.id, 112233)
    end

    private

    def retrieve_hierarchy_ids(direction=:ancestors, submission_id)
      current_level = 1
      max_levels = 40
      level_ids = Set.new([self.id])
      all_ids = Set.new()
      graphs = [submission_id]
      submission_id_string = submission_id
      while current_level <= max_levels do
        next_level = Set.new
        slices = level_ids.to_a.sort.each_slice(750).to_a
        threads = []
        slices.each_index do |i|
          ids_slice = slices[i]
          threads[i] = Thread.new {
            next_level_thread = Set.new
            query = hierarchy_query(direction,ids_slice)
            Goo.sparql_query_client.query(query,query_options: {rules: :NONE }, graphs: graphs)
                .each do |sol|
              parent = sol[:node].to_s
              next if !parent.start_with?("http")
              ontology = sol[:graph].to_s
              if submission_id_string == ontology
                unless all_ids.include?(parent)
                  next_level_thread << parent
                end
              end
            end
            Thread.current["next_level_thread"] = next_level_thread
          }
        end
        threads.each {|t| t.join ; next_level.merge(t["next_level_thread"]) }
        current_level += 1
        pre_size = all_ids.length
        all_ids.merge(next_level)
        if all_ids.length == pre_size
          #nothing new
          return all_ids
        end
        level_ids = next_level
      end
      return all_ids
    end

    def hierarchy_query(direction,class_ids)
      filter_ids = class_ids.map { |id| "?id = <#{id}>" } .join " || "
      directional_pattern = ""
      if direction == :ancestors
        directional_pattern = "?id <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?node . "
      else
        directional_pattern = "?node <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?id . "
      end

      query = <<-eos
        SELECT DISTINCT ?id ?node ?graph WHERE {
        GRAPH ?graph {
          #{directional_pattern}
        }
        FILTER (#{filter_ids})
        }
      eos

      return query
    end
  end
end