module ResourceIndex; end
RI = ResourceIndex

require 'goo'
require 'ostruct'
require 'sequel'
require 'typhoeus/adapters/faraday'
require_relative 'resource_index/version'
require_relative 'resource_index/resource'
require_relative 'resource_index/document'
require_relative 'resource_index/population/population'

module ResourceIndex
  HASH_SEED = 112233

  REQUIRED_OPTS = [:username, :password]
  def self.config(opts = {})
    raise ArgumentError, "You need to pass db_opts for #{self.class.name}" unless opts && opts.is_a?(Hash)
    missing_opts = REQUIRED_OPTS - opts.keys
    raise ArgumentError, "Missing #{missing_opts.join(', ')} from db options" unless missing_opts.empty? || opts[:sqlite]
    opts[:host]     ||= "localhost"
    opts[:port]     ||= 3306
    opts[:database] ||= "resource_index"
    @opts = opts
    setup_sql_client

    # Elasticsearch
    es_host = opts[:es_host] || "localhost"
    es_port = opts[:es_port] || 9200
    @es     = ::Elasticsearch::Client.new(host: es_host, port: es_port, adapter: :typhoeus)
  end

  def self.es
    @es
  end

  def self.db
    @client
  end

  def self.refresh_db
    @client = Sequel.connect(@opts)
  end

  private

  def self.setup_sql_client
    if RUBY_PLATFORM == "java"
      @opts[:adapter] = "jdbc"
      @opts[:uri] = "jdbc:mysql://#{@opts[:host]}:#{@opts[:port]}/#{@opts[:database]}?user=#{@opts[:username]}&password=#{@opts[:password]}"
    end
    @opts[:adapter] ||= "mysql2"
    @client = Sequel.connect(@opts)
  end
end
