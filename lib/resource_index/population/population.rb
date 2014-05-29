require 'logger'
require 'ostruct'
require 'json'
require 'zlib'

module RI::Population; end

require_relative 'goo_config'
require_relative 'elasticsearch'
require_relative 'mgrep/mgrep'
require_relative 'label_converter'
require_relative 'persisted_hash'

class RI::Population::Manager
  include RI::Population::GooConfig
  include RI::Population::Elasticsearch

  def initialize(res, opts = {})
    raise ArgumentError, "Please provide a resource" unless res.is_a?(RI::Resource)

    @res                   = res
    @settings              = s = OpenStruct.new
    s.annotator_redis_host = opts[:annotator_redis_host] || "localhost"
    s.annotator_redis_port = opts[:annotator_redis_port] || 6379
    s.mgrep_host           = opts[:mgrep_host] || "localhost"
    s.mgrep_port           = opts[:mgrep_port] || 55555
    s.population_threads   = opts[:population_threads] || 1
    s.ancestors_dumps_dir  = opts[:ancestors_dumps_dir] || Dir.pwd
    s.es_url               = opts[:es_url] || "http://localhost:9200"
    s.bulk_index_size      = opts[:bulk_index_size] || 100

    @logger                = opts[:logger] || Logger.new(STDOUT)
    @es                    = Elasticsearch::Client.new(url: @es_url)
    @mgrep                 = opts[:mgrep_client] || Annotator::Mgrep::ThreadedClient.new(s.mgrep_host, s.mgrep_port)
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

  def self.mutex
    @@mutex
  end

  def settings
    @settings
  end

  def populate(opts = {})
    delete_old = opts[:delete_old] || false
    begin
      @logger.debug "Starting population"
      @logger.debug "Creating new index"
      create_index()
      @logger.debug "Indexing documents"
      index_documents()
      @logger.debug "Aliasing index"
      alias_index()
      if delete_old
        @logger.debug "Removing old (unaliased) indices"
        delete_unaliased()
      end
      @logger.debug "Population complete"
    rescue => e
      @logger.error "Error populating resource #{@res.acronym}"
      @logger.error "#{e.message}\n#{e.backtrace.join("\n\t")}"
      alias_error()
    end
  end

  private

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

  def ancestors_cache
    @@ancestors
  end

  def annotated_classes(doc)
    annotations = @mgrep.annotate(doc.annotatable_text, false)
    string_ids = Set.new
    annotations.each {|a| string_ids << a.string_id.to_i}
    @label_converter.convert(string_ids)
  end
end
