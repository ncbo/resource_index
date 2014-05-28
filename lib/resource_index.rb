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

  REQUIRED_OPTS = [:username, :password]
  def self.config(opts = {})
    raise ArgumentError, "You need to pass db_opts for #{self.class.name}" unless opts && opts.is_a?(Hash)
    missing_opts = REQUIRED_OPTS - opts.keys
    raise ArgumentError, "Missing #{missing_opts.join(', ')} from db options" unless missing_opts.empty?
    opts[:host]     ||= "localhost"
    opts[:port]     ||= 3306
    opts[:database] ||= "resource_index"
    setup_sql_client(opts)
  end

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
