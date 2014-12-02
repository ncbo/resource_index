require 'set'
require_relative 'persisted_hash'

module RI::Population::Indexing
  class RetryError < StandardError
    attr_accessor :retry_count, :original_error

    def initialize(*args)
      @original_error = self
      super
    end
  end

  def index_documents(offset = 0)
    begin
      es_threads = []
      count = offset || 0
      documents = RI::Population::Document.all(@res, {offset: count}, @mutex)

      # We add code blocks to the lazy enumerable here
      # the code isn't evaluated until you call .to_a or .each
      # or otherwise force the enumerable to be eval'ed
      threads = documents.collect do |doc|
        Thread.new do
          annotation_time = 0
          ancestors_time = 0
          retry_count = 0
          begin
            annotations = {direct: Set.new, ancestors: Set.new}
            index_doc = nil
            @mutex.synchronize { index_doc = doc.indexable_hash }
            annotation_start = Time.now
            classes, labels = annotated_classes(doc)
            classes = classes + index_doc.delete(:manual_annotations)
            annotation_time += Time.now - annotation_start
            seen_classes = Set.new
            classes.each do |cls|
              next if seen_classes.include?(cls)
              seen_classes << cls

              next if @settings.skip_es_storage # skip if we don't index

              ancestors = nil
              RI::Population::Manager.mutex.synchronize { ancestors = ancestors_cache[cls.xxhash] }
              unless ancestors
                submission_id = "http://data.bioontology.org/ontologies/#{cls.ont_acronym}/submissions/#{latest_submissions[cls.ont_acronym]}"
                ancestors_start = Time.now
                ancestors = cls.retrieve_ancestors(cls.ont_acronym, submission_id)
                ancestors_time += Time.now - ancestors_start
                RI::Population::Manager.mutex.synchronize { ancestors_cache[cls.xxhash] = ancestors }
              end

              annotations[:direct].add(cls.xxhash)
              annotations[:ancestors].merge(ancestors) if ancestors
            end

            # Write data to file for co-occurence calculation
            if @settings.write_label_pairs
              write_label_pairs(labels)
            end

            # Switch the annotaions to an array
            index_doc[:annotations] = annotations

            # Add to batch index, push to ES if we hit the chunk size limit
            @mutex.synchronize {
              unless @settings.skip_es_storage
                @es_queue << index_doc

                if @es_queue.length >= settings.bulk_index_size
                  @logger.debug "Indexing docs @ #{count}"
                  es_threads << Thread.new do
                    store_documents(count)
                  end
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

          @logger.debug "Doc #{doc.id} annotations time: #{annotation_time.to_f.round(2)}s"
          @logger.debug "Doc #{doc.id} ancestors time: #{ancestors_time.to_f.round(2)}s"
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

  def store_documents(count = nil)
    es_queue = nil
    @mutex.synchronize {
      es_queue = @es_queue.dup
      @es_queue = []
    }
    return if es_queue.empty?
    @logger.info "Storing #{es_queue.length} records in #{index_id}"
    @logger.info "Processed documents: #{count} / #{@res.count}" if count
    bulk_items = []
    es_queue.each do |doc|
      doc[:annotations][:direct] = doc[:annotations][:direct].to_a
      doc[:annotations][:ancestors] = doc[:annotations][:ancestors].to_a
      bulk_items << {index: {_index: index_id, _type: "#{@res.acronym.downcase}_doc", _id: doc[:id], data: doc}}
    end
    return if bulk_items.empty?
    retries = 0
    begin
      @es.bulk body: bulk_items
    rescue => e
      sleep(3)
      retries += 1
      retry if retries <= 5
      store_documents
      raise e
    end
  end

  def annotated_classes(doc)
    annotations = @mgrep.annotate(doc.annotatable_text, false)
    string_ids = Set.new
    labels = Set.new
    annotations_objs = Set.new
    annotations.each do |a|
      next unless a.value.length >= 4 # Skip any annotation less than four characters long
      next if (Float(a) != nil rescue false) # Skip any integer or float value

      string_ids << a.string_id.to_i
      labels << a.value.downcase
      annotations_objs << a
    end
    return @label_converter.convert(string_ids, annotations_objs), labels
  end

  def write_label_pairs(labels)
    return unless @settings.write_label_pairs
    sorted_labels = labels.sort
    size = sorted_labels.size
    for i in 0...size
      for j in 0...i
        @mutex_label_pairs.synchronize { @labels_file.puts(sorted_labels[i] + "\t" + sorted_labels[j]) }
      end
    end
  end

end