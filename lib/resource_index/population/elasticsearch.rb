require 'dbm'
require 'elasticsearch'
require_relative 'persisted_hash'

module RI::Population::Elasticsearch
  def index_id
    "#{@res.acronym.downcase}_#{@time.to_i}"
  end

  def index_documents(offset = 0)
    begin
      count = offset
      RI::Document.threach(@res, {thread_count: settings.population_threads, offset: count}, @mutex) do |doc|
        annotations = {}
        annotated_classes(doc).each do |cls|
          if annotations[cls.xxhash]
            annotations[cls.xxhash][:count] += 1
            next
          end

          ancestors = nil
          RI::Population::Manager.mutex.synchronize { ancestors = ancestors_cache[cls.xxhash] }
          unless ancestors
            submission_id = "http://data.bioontology.org/ontologies/#{cls.ont_acronym}/submissions/#{latest_submissions[cls.ont_acronym]}"
            ancestors = cls.retrieve_ancestors(cls.ont_acronym, submission_id)
            RI::Population::Manager.mutex.synchronize { ancestors_cache[cls.xxhash] = ancestors }
          end
          annotations[cls.xxhash] = {direct: cls.xxhash, ancestors: ancestors, count: 1}
        end

        # Switch the annotaions to an array
        index_doc = doc.indexable_hash
        index_doc[:annotations] = annotations.values

        # Add to batch index, push to ES if we hit the chunk size limit
        @mutex.synchronize {
          @es_queue << index_doc

          if @es_queue.length >= settings.bulk_index_size
            @logger.debug "Indexing docs @ #{count}"
            store_documents
          end

          count += 1
          @logger.debug "Doc count: #{count}" if count % 10 == 0
        }
      end
    rescue => e
      store_documents # store any remaining in the queue
      @logger.warn "Saving place in population for later resuming at record #{count}"
      save_for_resume(count)
      raise e
    end

    store_documents # store any remaining in the queue
  end

  def create_index
    return if @es.indices.exists index: index_id
    @es.indices.create index: index_id, type: "#{@res.acronym.downcase}_doc", body: es_mapping
    @es.indices.put_alias index: index_id, name: "#{@res.acronym}_populating"
  end

  def alias_index
    previous = (@es.indices.get_alias name: @res.acronym).keys.first rescue nil # get the prior index
    @es.indices.put_alias index: previous, name: "#{@res.acronym}_previous" if previous # add RES_previous alias for easy rollback
    old_aliases = @es.indices.get_aliases.select {|k,v| v["aliases"].key?(@res.acronym)} # list of anything else with the alias (safety check)
    old_aliases.each {|k,v| @es.indices.delete_alias index: k, name: @res.acronym} # delete the old stuff
    @es.indices.put_alias index: index_id, name: @res.acronym # name new index
    @es.indices.delete_alias index: index_id, name: "#{@res.acronym}_populating" # remove populating
  end

  def alias_error
    @es.indices.put_alias index: index_id, name: "error"
  end

  def delete_unaliased
    indices = @es.indices.get_aliases index: "#{@res.acronym.downcase}*"
    indices.each {|index_id, hsh| @es.indices.delete index: index_id if hsh["aliases"].empty? || (hsh["aliases"].key?("error"))}
  end

  def store_documents
    @logger.debug "Storing #{@es_queue.length} records in #{index_id}"
    bulk_items = []
    @es_queue.each do |doc|
      bulk_items << {index: {_index: index_id, _type: "#{@res.acronym.downcase}_doc", _id: doc[:id], data: doc}}
    end
    @es.bulk body: bulk_items
    @es_queue = []
  end

  def es_mapping
    properties_json = Hash[@res.fields.keys.map {|f| [f.downcase.to_sym, {type: :string}] }]
    {
      mappings: {
        :"#{@res.acronym.downcase}_doc" => {
          :"_source" => {
            includes: @res.fields.keys.map {|f| f.downcase.to_sym},
            excludes: [:annotations]
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
end
