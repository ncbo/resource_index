require 'logger'
require 'ostruct'
require 'json'
require 'zlib'
require 'typhoeus/adapters/faraday'

module RI::Population; end

require_relative 'goo_config'
require_relative 'elasticsearch'
require_relative 'mgrep/mgrep'
require_relative 'label_converter'
require_relative 'persisted_hash'
require_relative 'notification'
require_relative 'indexing'

class RI::Population::Manager
  include RI::Population::GooConfig
  include RI::Population::Elasticsearch
  include RI::Population::Indexing
  include RI::Population::Notification

  def initialize(res, opts = {})
    raise ArgumentError, "Please provide a resource" unless res.is_a?(RI::Resource)

    @res                   = res
    @settings              = s = OpenStruct.new
    s.annotator_redis_host = opts[:annotator_redis_host] || "localhost"
    s.annotator_redis_port = opts[:annotator_redis_port] || 6379
    s.mgrep_host           = opts[:mgrep_host] || "localhost"
    s.mgrep_port           = opts[:mgrep_port] || 55555
    s.population_threads   = opts[:population_threads] || 1
    s.dumps_dir            = opts[:dumps_dir] || Dir.pwd
    s.es_hosts             = opts[:es_hosts] || ["localhost"]
    s.es_port              = opts[:es_port] || 9200
    s.bulk_index_size      = opts[:bulk_index_size] || 100
    s.starting_offset      = opts[:starting_offset] || 0
    s.resume               = opts[:resume].nil? ? true : opts[:resume]

    s.es_hosts = s.es_hosts.is_a?(Array) ? s.es_hosts : [s.es_hosts]

    @logger                = opts[:logger] || Logger.new(STDOUT)
    @logger.level          = opts[:log_level] || $ri_log_level || Logger::INFO
    @es                    = ::Elasticsearch::Client.new(hosts: s.es_hosts, port: s.es_port, adapter: :typhoeus)
    @mgrep                 = opts[:mgrep_client] || RI::Population::Mgrep::ThreadedClient.new(s.mgrep_host, s.mgrep_port)
    @label_converter       = RI::Population::LabelConverter.new(s.annotator_redis_host, s.annotator_redis_port)
    @mutex                 = Mutex.new
    @es_queue              = []
    @time                  = Time.at(opts[:time_int] || Time.now)

    @@mutex                = Mutex.new

    @@mutex.synchronize {
      @@ancestors ||= Persisted::Hash.new("ri_pop_anc", dir: s.dumps_dir, gzip: true)
    }

    # Mail notification settings
    @smtp_host       = opts[:smtp_host]      || "smtp-unencrypted.stanford.edu"
    @smtp_port       = opts[:smtp_port]      || 25
    @smtp_auth_type  = opts[:smtp_auth_type] || :none # :none, :plain, :login, :cram_md5
    @smtp_domain     = opts[:smtp_domain]    || "localhost.localhost"
    @smtp_user       = opts[:smtp_user]
    @smtp_password   = opts[:smtp_password]
    @mail_recipients = opts[:mail_recipients]

    goo_setup(opts)

    # Manual resume trigger
    save_for_resume(s.starting_offset) if s.starting_offset > 0 && opts[:time_int]

    # Resume from previous population
    if s.resume && File.exist?(resume_path)
      resumed = Marshal.load(File.read(resume_path))
      s.starting_offset = resumed[:count]
      @time = resumed[:time]
      remove_error_alias()
      @logger.warn "Resuming process for #{@res.acronym} at record #{s.starting_offset}"
      File.delete(resume_path)
    end

    # Close mgrep connections at exit
    if @mgrep.is_a?(RI::Population::Mgrep::ThreadedClient)
      at_exit do
        @mgrep.close_all
      end
    end

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
      index_documents(@settings.starting_offset)
      @logger.debug "Aliasing index"
      alias_index()
      if delete_old
        @logger.debug "Removing old (unaliased) indices"
        delete_unaliased()
      end
      @logger.debug "Population complete"
      success_email
    rescue => e
      @logger.error "Error populating resource #{@res.acronym}"
      alias_error()
      error_email(e)
      raise e
    end
    index_id()
  end

  private

  def save_for_resume(count, time = nil)
    time ||= @time
    File.open(resume_path, 'w') do |f|
      f.write Marshal.dump(count: count, time: time)
      f.close
    end
  end

  def resume_path
    Dir.pwd + "/#{@res.acronym.downcase}_index_resume"
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
