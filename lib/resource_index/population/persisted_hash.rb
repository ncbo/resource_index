require 'forwardable'
require 'zlib'

module Persisted
  class Hash
    extend Forwardable

    class CopyError < StandardError; end
    class ExistsError < StandardError; end

    def_delegators :@hash, *(::Hash.public_methods(false) - [:"[]=", :merge, :"merge!", :invert])

    def initialize(name,
                   persist_write_count: 10000,
                   gzip: false,
                   hash: nil,
                   dir: nil,
                   allow_gc: false)
      @hash = hash || {}
      @allow_gc = allow_gc
      @write_count = 0
      @dir ||= Dir.pwd
      @name = name
      path = @dir + "/#{@name}_persisted_hash.dump"
      @base_path = File.expand_path(path)
      @persist_write_count = persist_write_count
      @gzip = gzip

      if File.exist?(path())
        load
      end

      # Make sure object sticks around until exit
      # This can be overridden by passing allow_gc: true
      @@persisted_active ||= {}
      unless @allow_gc
        @@persisted_active[self] = true
        at_exit do
          write
        end
      end
    end

    ##
    # Allow the garbage collector to reap the object
    def free
      return false unless allow_gc
      write
      @@persisted_active.delete(self)
      @freed = true
      @hash.freeze
      true
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

    ##
    # Don't allow copying
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
      Kernel.warn("WARNING: Modifying freed #{self.class.name} (#{@name}), forcing write to disk")
      if @write_count >= @persist_write_count || @freed
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
