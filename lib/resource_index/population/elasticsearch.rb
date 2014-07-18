require 'elasticsearch'
require_relative 'persisted_hash'

module RI::Population::Elasticsearch
  class RetryError < StandardError
    attr_accessor :retry_count, :original_error

    def initialize(*args)
      @original_error = self
      super
    end
  end

  def index_id
    "#{@res.acronym.downcase}_#{@time.to_i}"
  end

  def index_documents(offset = 0)
    begin
      es_threads = []
      count = offset
      documents = RI::Document.all(@res, {offset: count}, @mutex)

      # We add code blocks to the lazy enumerable here
      # the code isn't evaluated until you call .to_a or .each
      # or otherwise force the enumerable to be eval'ed
      threads = documents.collect do |doc|
        Thread.new do
          retry_count = 0
          begin
            annotations = {}
            index_doc = nil
            @mutex.synchronize { index_doc = doc.indexable_hash }
            classes = annotated_classes(doc) + index_doc.delete(:manual_annotations)
            classes.each do |cls|
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
            index_doc[:annotations] = annotations.values

            # Add to batch index, push to ES if we hit the chunk size limit
            @mutex.synchronize {
              @es_queue << index_doc

              if @es_queue.length >= settings.bulk_index_size
                @logger.debug "Indexing docs @ #{count}"
                es_threads << Thread.new do
                  store_documents
                end
              end

              count += 1
              @logger.debug "Doc count: #{count}" if count % 10 == 0
              @last_processed_id = doc.id
            }
          rescue => e
            retry_count += 1
            unless retry_count >= 5
              @logger.warn "Retrying, attempt #{retry_count} (#{e.message})"
              retry
            end
            @logger.warn "Retried but failed to fix"
            sleep(3)
            err = RetryError.new(e)
            err.retry_count = retry_count
            err.original_error = e unless e.is_a?(RetryError)
            raise err
          end
        end
      end

      # This construct is used because you can't access enumerable
      # from new threads because of the Fibers used in enum
      # This will enumerate over the enum and run as many threads as
      # is configured by the population process at a time.
      running = true
      workers = []
      while running || !workers.empty?
        workers.each(&:join)
        workers = []
        begin
          settings.population_threads.times {workers << threads.next}
        rescue StopIteration
          running = false
        end
      end
    rescue => e
      e = e.original_error if e.is_a?(RetryError)
      @logger.warn "Saving place in population for later resuming at record #{count}"
      @logger.error e.message
      @logger.error e.backtrace.join("\n\t")
      store_documents # store any remaining in the queue
      save_for_resume(@last_processed_id) if settings.resume
      raise e
    end

    es_threads.each(&:join)

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

  def remove_error_alias
    begin
      @es.indices.delete_alias index: index_id, name: "error"
    rescue Elasticsearch::Transport::Transport::Errors::NotFound
      # Alias not found, move on
    end
  end

  def store_documents
    es_queue = nil
    @mutex.synchronize {
      es_queue = @es_queue.dup
      @es_queue = []
    }
    return if es_queue.empty?
    @logger.debug "Storing #{es_queue.length} records in #{index_id}"
    bulk_items = []
    es_queue.each do |doc|
      bulk_items << {index: {_index: index_id, _type: "#{@res.acronym.downcase}_doc", _id: doc[:id], data: doc}}
    end
    return if bulk_items.empty?
    retries = 0
    begin
      @es.bulk body: bulk_items
    rescue => e
      sleep(3)
      retries += 1
      retry if retries < 5
      raise e
    end
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
