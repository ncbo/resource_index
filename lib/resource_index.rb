module ResourceIndex; end
RI = ResourceIndex

require 'sequel'
require_relative 'resource_index/version'
require_relative 'resource_index/resource'

module ResourceIndex
  class Base
    REQUIRED_OPTS = [:username, :password]
    def initialize(opts = {})
      raise ArgumentError, "You need to pass db_opts for #{self.class.name}" unless opts && opts.is_a?(Hash)
      missing_opts = REQUIRED_OPTS - opts.keys
      raise ArgumentError, "Missing #{missing_opts.join(', ')} from db options" unless missing_opts.empty?
      opts[:host]     ||= "localhost"
      opts[:port]     ||= 3306
      opts[:database] ||= "resource_index"
      setup_sql_client(opts)
    end

    def resources
      res = db[:obr_resource]
      res.all.map {|r| RI::Resource.new(r.values)}
    end

    def resource(res)
      res = db[:obr_resource].where(resource_id: res).limit(1).first
      RI::Resource.new(res)
    end

    def db
      @client
    end

    private

    def setup_sql_client(opts = {})
      if RUBY_PLATFORM == "java"
        opts = opts.dup
        opts[:adapter] = "jdbc"
        opts[:uri] = "jdbc:mysql://#{opts[:host]}:#{opts[:port]}/#{opts[:database]}?user=#{opts[:username]}&password=#{opts[:password]}"
      end
      opts[:adapter] ||= "mysql2"
      @client = Sequel.connect(opts)
    end

  end
end
