require 'logger'
require 'elasticsearch'
require 'ostruct'
require 'json'
require 'zlib'
require_relative 'goo'
require_relative 'mgrep/mgrep'
require_relative 'label_converter'
require_relative 'persisted_hash'

module RI::Population; end

class RI::Population::Manager
  include RI::Population::Goo

  ALL_ANCESTORS_FILE = "ancestors.gz"

  def initialize(res, opts = {})
    raise ArgumentError, "Please provide a resource" unless res.is_a?(RI::Resource)

    @res                   = res
    @settings              = s = OpenStruct.new
    s.annotator_redis_host = opts[:annotator_redis_host] || "localhost"
    s.annotator_redis_port = opts[:annotator_redis_port] || 6379
    s.mgrep_host           = opts[:mgrep_host] || "localhost"
    s.mgrep_port           = opts[:mgrep_port] || 55555
    s.population_threads   = opts[:population_threads] || 2
    s.ancestors_dumps_dir  = opts[:ancestors_dumps_dir] || Dir.pwd
    s.es_url               = opts[:es_url] || "http://localhost:9200"
    s.bulk_index_size      = opts[:bulk_index_size] || 100

    @logger                = opts[:logger] || Logger.new(STDOUT)
    @es                    = Elasticsearch::Client.new(url: @es_url)
    @mgrep                 = client = Annotator::Mgrep::Client.new(s.mgrep_host, s.mgrep_port)
    @label_converter       = RI::Population::LabelConverter.new(s.annotator_redis_host, s.annotator_redis_port)
    @mutex                 = Mutex.new
    @es_queue              = []
    @time                  = Time.now

    @@mutex                = Mutex.new

    @@mutex.synchronize {
      @@ancestors ||= Persisted::Hash.new("ri_pop_anc", dir: s.ancestors_dumps_dir, gzip: true)
    }

    goo_setup(opts)

    nil
  end

  def settings
    @settings
  end

  def populate
    @logger.debug "Starting population"
    @logger.debug "Creating new index"
    create_index
    @logger.debug "Getting documents"
    index_documents
    @logger.debug "Aliasing index"
    alias_index
  end

  private

  def index_id
    "#{@res.acronym.downcase}_#{@time.to_i}"
  end

  def latest_submissions
    @latest_submissions ||= latest_submissions_sparql
  end

  def latest_submissions_sparql
    sub_id_query = <<-EOS
    PREFIX bp: <http://data.bioontology.org/metadata/>

    SELECT ?s ?sId WHERE {
      ?s a bp:OntologySubmission .
      ?s bp:submissionId ?sId .
    }
    EOS
    submission_ids = Goo.sparql_query_client.query(sub_id_query)
    latest_sub = {}
    submission_ids.each do |sub|
      acronym = sub[:s].to_s.split("/")[4]
      id = sub[:sId].to_i
      latest_sub[acronym] ||= -1
      latest_sub[acronym] = id if id > latest_sub[acronym]
    end
    latest_sub
  end

  def ancestors_hash
    @@ancestors
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
        @@mutex.synchronize { ancestors = ancestors_hash[cls.xxhash] }
        unless ancestors
          submission_id = "http://data.bioontology.org/ontologies/#{cls.ont_acronym}/submissions/#{latest_submissions[cls.ont_acronym]}"
          ancestors = cls.retrieve_ancestors(cls.ont_acronym, submission_id)
          @@mutex.synchronize { ancestors_hash[cls.xxhash] = ancestors }
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

  def annotated_classes(doc)
    annotations = @mgrep.annotate(doc.annotatable_text, false)
    string_ids = Set.new
    annotations.each {|a| string_ids << a.string_id.to_i}
    @label_converter.convert(string_ids)
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
    es.bulk body: bulk_queue
    @es_queue = {}
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

<<-EOS
{
    "mappings": {
        "%doc_type%": {
            "_source" : {
              "includes": ["title", "abstract"],
              "excludes": ["annotations"]
            },
            "properties": {
                %properties_json%,
                "annotations": {
                    "type": "nested",
                    "include_in_all": false,
                    "properties": {
                        "direct": {"type": "long", "store": false, "include_in_all": false},
                        "ancestors": {"type": "long", "store": false, "include_in_all": false},
                        "count": {"type": "long", "store": false, "include_in_all": false}
                    }
                }
            }
        }
    }
}
EOS
end
