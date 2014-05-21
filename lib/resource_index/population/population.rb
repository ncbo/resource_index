require 'logger'
require 'goo'
require 'elasticsearch'
require 'ostruct'
require 'json'
require 'zlib'
require_relative 'mgrep/mgrep'
require_relative 'label_converter'
require_relative 'persisted_hash'

module RI::Population; end

class RI::Population::Manager
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

  def goo_setup(opts)
    # Set defaults
    @settings.goo_port          = opts[:goo_port] || 9000
    @settings.goo_host          = opts[:goo_host] || "localhost"
    @settings.search_server_url = opts[:search_server_url] || "http://localhost:8983/solr"
    @settings.enable_goo_cache  = opts[:enable_goo_cache] || false

    goo_namespaces
    goo_connect
  end

  ##
  # Connect to goo by configuring the store and search server
  def goo_connect
    port              ||= @settings.goo_port
    host              ||= @settings.goo_host

    begin
      Goo.use_cache = @settings.enable_goo_cache
      Goo.configure do |conf|
        conf.queries_debug(@settings.queries_debug)
        conf.add_sparql_backend(:main, query: "http://#{host}:#{port}/sparql/",
                                data: "http://#{host}:#{port}/data/",
                                update: "http://#{host}:#{port}/update/",
                                options: { rules: :NONE })

        conf.add_search_backend(:main, service: @settings.search_server_url)
        conf.add_redis_backend(host: @settings.goo_redis_host,
                               port: @settings.goo_redis_port)
      end
    rescue Exception => e
      abort("EXITING: Cannot connect to triplestore and/or search server:\n  #{e}\n#{e.backtrace.join("\n")}")
    end
  end

  ##
  # Configure ontologies_linked_data namespaces
  # We do this at initial runtime because goo needs namespaces for its DSL
  def goo_namespaces
    Goo.configure do |conf|
      conf.add_namespace(:omv, RDF::Vocabulary.new("http://omv.ontoware.org/2005/05/ontology#"))
      conf.add_namespace(:skos, RDF::Vocabulary.new("http://www.w3.org/2004/02/skos/core#"))
      conf.add_namespace(:owl, RDF::Vocabulary.new("http://www.w3.org/2002/07/owl#"))
      conf.add_namespace(:rdfs, RDF::Vocabulary.new("http://www.w3.org/2000/01/rdf-schema#"))
      conf.add_namespace(:metadata, RDF::Vocabulary.new("http://data.bioontology.org/metadata/"), default = true)
      conf.add_namespace(:metadata_def, RDF::Vocabulary.new("http://data.bioontology.org/metadata/def/"))
      conf.add_namespace(:dc, RDF::Vocabulary.new("http://purl.org/dc/elements/1.1/"))
      conf.add_namespace(:xsd, RDF::Vocabulary.new("http://www.w3.org/2001/XMLSchema#"))
      conf.add_namespace(:oboinowl_gen, RDF::Vocabulary.new("http://www.geneontology.org/formats/oboInOWL#"))
      conf.add_namespace(:obo_purl, RDF::Vocabulary.new("http://purl.obolibrary.org/obo/"))
      conf.add_namespace(:umls, RDF::Vocabulary.new("http://bioportal.bioontology.org/ontologies/umls/"))
      conf.id_prefix = "http://data.bioontology.org/"
      conf.pluralize_models(true)
    end
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
