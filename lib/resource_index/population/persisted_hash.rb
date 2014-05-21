require 'forwardable'
require 'zlib'

module Persisted
  class Hash
    extend Forwardable

    class CopyError < StandardError; end
    class ExistsError < StandardError; end

    def_delegators :@hash, *(::Hash.public_methods(false) - [:"[]=", :merge, :"merge!", :invert])

    def self.finalize(object_id)
      inst = ObjectSpace._id2ref(object_id)
      puts "Writing #{inst.name} (id #{inst.object_id}) at #{Time.now}"
      inst.write
    end

    def initialize(name,
                   persist_write_count: 10000,
                   gzip: false,
                   hash: nil,
                   dir: nil)
      @hash = hash || {}
      @write_count = 0
      @dir ||= Dir.pwd
      @name = name
      path = @dir + "/#{@name}_persisted_hash.dump"
      @base_path = File.expand_path(path)
      @persist_write_count = persist_write_count
      @gzip = gzip
      if File.exist?(path)
        load
      end

      # Make sure object gets written when it's collected
      ObjectSpace.define_finalizer(self, self.class.method(:finalize).to_proc)
    end

    def write
      if @gzip
        File.open(path, 'w') do |f|
          gz = Zlib::GzipWriter.new(f)
          gz.write Marshal.dump(@hash)
          gz.close
        end
      else
        File.open(path, 'w') do |f|
          f.write Marshal.dump(@hash)
          f.close
        end
      end
    end

    def name
      @name
    end

    def []=(key, value)
      count_writes
      ret_val = @hash[key] = value
      write_check
      ret_val
    end

    def merge!(hash)
      count_writes(hash.keys.length)
      ret_val = @hash.merge!(hash)
      write_check
      ret_val
    end

    ## Don't allow copying
    def nodup(*args)
      raise CopyError, "#{self.class.name} cannot be copied, duplicated or cloned"
    end
    alias :invert :nodup
    alias :merge :nodup
    alias :dup :nodup
    alias :clone :nodup

    private

    def path
      @base_path + (@gzip ? ".gz" : "")
    end

    def count_writes(num = 1)
      @write_count += num
    end

    def write_check
      if @write_count >= @persist_write_count
        write
        @write_count = 0
      end
    end

    def load
      if @gzip
        Zlib::GzipReader.open(path) do |gz|
          @hash = Marshal.load(gz.read)
        end
      else
        @hash = Marshal.load(File.read(path))
      end
    end
  end
end
