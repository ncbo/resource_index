require 'logger'
require 'ostruct'
require 'json'
require 'zlib'
require 'typhoeus/adapters/faraday'
require 'open3'

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
    s.skip_es_storage      = opts[:skip_es_storage]
    s.write_singlets       = opts[:write_singlets]
    s.write_cofreqs        = opts[:write_cofreqs]
    s.extraction_output    = opts[:extraction_output] || File.join(Dir.pwd, 'extraction_results')

    s.es_hosts = s.es_hosts.is_a?(Array) ? s.es_hosts : [s.es_hosts]

    @logger                = opts[:logger] || Logger.new(STDOUT)
    @logger.level          = opts[:log_level] || $ri_log_level || Logger::INFO
    @es                    = ::Elasticsearch::Client.new(hosts: s.es_hosts, port: s.es_port, adapter: :typhoeus)
    @mgrep                 = opts[:mgrep_client] || RI::Population::Mgrep::ThreadedClient.new(s.mgrep_host, s.mgrep_port)
    @label_converter       = RI::Population::LabelConverter.new(s.annotator_redis_host, s.annotator_redis_port)
    @mutex                 = Mutex.new
    @mutex_cofreqs         = Mutex.new
    @mutex_singlets        = Mutex.new
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
      remove_error_alias() unless s.skip_es_storage
      @logger.warn "Resuming process for #{@res.acronym} at record #{s.starting_offset}"
      File.delete(resume_path)
    end

    # Close mgrep connections at exit
    if @mgrep.is_a?(RI::Population::Mgrep::ThreadedClient)
      at_exit do
        @mgrep.close_all
      end
    end

    # Setup files for writing co-frequency data (as needed)
    if s.write_cofreqs
      path = cofreqs_path()
      FileUtils.mkdir_p(File.dirname(path))
      @cofreqs_file = File.new(path, 'a')

      counts_path = cofreqs_counts_path
      FileUtils.mkdir_p(File.dirname(counts_path))
      @cofreqs_counts_file = File.new(counts_path, 'w')
    end

    # Setup files for writing singleton data (as needed)
    if s.write_singlets
      path = singlets_path()
      FileUtils.mkdir_p(File.dirname(path))
      @singlets_file = File.new(path, 'a')

      counts_path = singlets_counts_path()
      FileUtils.mkdir_p(File.dirname(counts_path))
      @singlets_counts_file = File.new(counts_path, 'w')
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
      unless @settings.skip_es_storage
        @logger.debug "Starting population"
        @logger.debug "Creating new index"
        create_index()
      end

      @logger.debug "Processing documents"
      index_documents(@settings.starting_offset)

      if @settings.write_cofreqs
        @cofreqs_file.close
        write_counts(cofreqs_path(), cofreqs_counts_path())
        @cofreqs_counts_file.close
      end

      if settings.write_singlets
        @singlets_file.close
        write_counts(singlets_path(), singlets_counts_path())
        @singlets_counts_file.close
      end

      unless @settings.skip_es_storage
        @logger.debug "Aliasing index"
        alias_index()
        if delete_old
          @logger.debug "Removing old (unaliased) indices"
          delete_unaliased()
        end
        @logger.debug "Population complete"
      end

      success_email
    rescue => e
      @logger.error "Error populating resource #{@res.acronym}"

      alias_error() unless @settings.skip_es_storage

      error_email(e)
      raise e
    end
    index_id()
  end

  def resume_path
    Dir.pwd + "/#{@res.acronym.downcase}_index_resume"
  end

  def extraction_dir
    File.join(@settings.extraction_output, @res.acronym)
  end

  def singlets_path
    File.join(extraction_dir(), index_id() + '_singlets.tsv')
  end

  def singlets_counts_path
    File.join(extraction_dir(), index_id() + '_singlets_counts.tsv')
  end

  def cofreqs_path
    File.join(extraction_dir(), index_id() + '_cofreqs.tsv')
  end

  def cofreqs_counts_path
    File.join(extraction_dir(), index_id() + '_cofreqs_counts.tsv')
  end

  def write_counts(input, output)
    # Using this regular expression in a sed command requires GNU sed. Won't work with OS X BSD sed.
    regex = '\'s/^([[:space:]]+[0-9]+)([[:space:]])/\1\t/\''

    `sort #{input} | uniq -c | sed -r #{regex} > #{output}`
    if not $?.success?
      @logger.error "Error generating #{output} for #{@res.acronym}: #{$?.to_s}"
    end
  end

  private

  def save_for_resume(count, time = nil)
    time ||= @time
    File.open(resume_path, 'w') do |f|
      f.write Marshal.dump(count: count, time: time)
      f.close
    end
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

end
