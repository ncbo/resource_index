module ResourceIndex; end
RI = ResourceIndex

require 'goo'
require 'ostruct'
require 'sequel'
require_relative 'resource_index/version'
require_relative 'resource_index/resource'
require_relative 'resource_index/document'
require_relative 'resource_index/population/population'

module ResourceIndex
  attr_reader :settings

  @settings = OpenStruct.new
  REQUIRED_OPTS = [:username, :password]

  def self.config(opts = {})
    raise ArgumentError, "You need to pass db_opts for #{self.class.name}" unless opts && opts.is_a?(Hash)
    missing_opts = REQUIRED_OPTS - opts.keys
    raise ArgumentError, "Missing #{missing_opts.join(', ')} from db options" unless missing_opts.empty?
    opts[:host]     ||= "localhost"
    opts[:port]     ||= 3306
    opts[:database] ||= "resource_index"
    setup_sql_client(opts)

    # Set defaults
    @settings.goo_port               ||= 9000
    @settings.goo_host               ||= "localhost"
    @settings.search_server_url      ||= "http://localhost:8983/solr"
    @settings.enable_goo_cache       ||= false

    connect_goo
  end

  ##
  # Connect to goo by configuring the store and search server
  def self.connect_goo
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

        if @settings.enable_monitoring
          puts "(LD) >> Enable SPARQL monitoring with cube #{@settings.cube_host}:"+
                    "#{@settings.cube_port}"
          conf.enable_cube do |opts|
            opts[:host] = @settings.cube_host
            opts[:port] = @settings.cube_port
          end
        end
      end
    rescue Exception => e
      abort("EXITING: Cannot connect to triplestore and/or search server:\n  #{e}\n#{e.backtrace.join("\n")}")
    end
  end

  ##
  # Configure ontologies_linked_data namespaces
  # We do this at initial runtime because goo needs namespaces for its DSL
  def self.goo_namespaces
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
  self.goo_namespaces

  def self.db
    @client
  end

  private

  def self.setup_sql_client(opts = {})
    if RUBY_PLATFORM == "java"
      opts = opts.dup
      opts[:adapter] = "jdbc"
      opts[:uri] = "jdbc:mysql://#{opts[:host]}:#{opts[:port]}/#{opts[:database]}?user=#{opts[:username]}&password=#{opts[:password]}"
    end
    opts[:adapter] ||= "mysql2"
    @client = Sequel.connect(opts)
  end
end
