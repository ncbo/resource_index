require 'elasticsearch'

module RI::Population::Elasticsearch
  def index_id
    "#{@res.acronym.downcase}_#{@time.to_i}"
  end

  def index_documents
    count = 0
    @res.documents.each do |doc|
      annotations = {}
      annotated_classes(doc).each do |cls|
        if annotations[cls.xxhash]
          annotations[cls.xxhash][:count] += 1
          next
        end

        ancestors = nil
        RI::Population::Manager.mutex.synchronize { ancestors = ancestors_hash[cls.xxhash] }
        unless ancestors
          submission_id = "http://data.bioontology.org/ontologies/#{cls.ont_acronym}/submissions/#{latest_submissions[cls.ont_acronym]}"
          ancestors = cls.retrieve_ancestors(cls.ont_acronym, submission_id)
          RI::Population::Manager.mutex.synchronize { ancestors_hash[cls.xxhash] = ancestors }
        end
        annotations[cls.xxhash] = {direct: cls.xxhash, ancestors: ancestors, count: 1}
      end

      # Switch the annotaions to an array
      index_doc = doc.indexable_hash
      index_doc[:annotations] = annotations.values

      # Add to batch index, push to ES if we hit the chunk size limit
      @mutex.synchronize {
        @es_queue << doc

        if @es_queue.length >= settings.bulk_index_size
          @logger.debug "Indexing docs"
          store_documents
        end
      }
      @logger.debug "Doc count: #{count += 1}"
    end
  end

  def create_index
    @es.indices.create index: index_id, type: @res.acronym, body: es_mapping
  end

  def alias_index
    @es.indices.put_alias index: index_id, name: @res.acronym
  end

  def store_documents
    bulk_items = []
    @es_queue.each do |doc|
      bulk_items << {index: {_index: index_id, _type: @res.acronym, _id: doc.id, data: doc}}
    end
    @es.bulk body: bulk_items
    @es_queue = []
  end

  def es_mapping
    properties_json = Hash[@res.fields.values.map {|f| [f.name.to_sym, {type: :string}] }]
    {
      mappings: {
        @res.acronym => {
          :"_source" => {
            includes: @res.fields.values.map {|f| f.name.to_sym},
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