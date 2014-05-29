require 'forwardable'
require 'zlib'
require 'ref'

module Persisted
  class Hash
    extend Forwardable

    class CopyError < StandardError; end
    class ExistsError < StandardError; end

    @@persist_data = true

    def_delegators :@data, *(::Hash.public_methods(false) - [:"[]=", :merge, :"merge!", :invert])

    ##
    # Look for objects that have been evicted from the system and
    # write their underlying data to disk.
    def self.poll_for_gced()
      while !@@ref_queue.empty?
        ref = @@ref_queue.shift
        data = @@data_refs.delete(ref.referenced_object_id)
        inst = self.new_from_data_ref(data)
        puts "Writing #{inst.class.name} (#{inst.name}) to disk after it was garbage collected"
        inst.write
      end
    end

    ##
    # This holds underlying data in order to reconstruct
    # original Persisted::Hash objects so they can be synced
    # to disk even after the original object has been GCed
    # Data to recreate the object is stored on the class layer.
    # This allows the Persisted objects to get GCed but we'll
    # still have a reference to the data.
    @@data_refs ||= {}

    ##
    # @@ref_queue is used to monitor objects and gets populated
    # when a monitored object is evicted by the GC
    @@ref_queue = Ref::ReferenceQueue.new
    @@ref_map = Ref::WeakValueMap.new

    ##
    # The polling thread will continuisly look for GCed objects
    # and recreate them for storage on disk
    @@poll_thread ||= Thread.new do
      while
        sleep(0.5)
        poll_for_gced()
      end
    end

    ##
    # The at_exit hook will write out any outstanding objects when
    # the VM exits. Could prevent the VM from exiting if objects aren't freed
    # for some reason (don't think that should happen)
    at_exit do
      @@ref_map.values.each do |ref|
        data = @@data_refs.delete(ref.object_id)
        inst = ref.class.new_from_data_ref(data)
        puts "Writing #{inst.class.name} (#{inst.name}) to disk at exit"
        inst.write
      end
    end

    def initialize(name, opts = {})
      opts = {
        persist_write_count: 10000,
        gzip: false,
        data: nil,
        dir: nil,
        allow_gc: false
      }.merge(opts)
      @name                = name
      @allow_gc            = opts[:allow_gc]
      @persist_write_count = opts[:persist_write_count]
      @gzip                = opts[:gzip]
      @dir                 ||= Dir.pwd
      path                 = @dir + "/#{@name}_persisted_hash.dump"
      @base_path           = File.expand_path(path)
      @write_count         = 0
      load(data)
      self
    end

    def data
      @data
    end

    def to_data_ref(data)
      {data: data, name: @name.dup, gzip: @gzip, dir: @dir.dup}
    end

    def self.new_from_data_ref(ref)
      self.new(ref[:name], data: ref[:hash], dir: ref[:dir], gzip: ref[:gzip])
    end

    def self.prevent_persist
      puts @@persist_data
      @@persist_data = false
      puts @@persist_data
    end

    def write
      return unless @@persist_data
      if @gzip
        File.open(path, 'w') do |f|
          gz = Zlib::GzipWriter.new(f)
          gz.write Marshal.dump(data())
          gz.close
        end
      else
        File.open(path, 'w') do |f|
          f.write Marshal.dump(data())
          f.close
        end
      end
    end

    def name
      @name
    end

    def []=(key, value)
      count_writes
      ret_val = data()[key] = value
      write_check
      ret_val
    end

    def merge!(hash)
      count_writes(hash.keys.length)
      ret_val = data().merge!(hash)
      write_check
      ret_val
    end

    def to_hash
      @data
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
      if @write_count >= @persist_write_count
        write
        @write_count = 0
      end
    end

    def load(data)
      if @@persist_data
        if File.exist?(path()) && data.nil?
          if @gzip
            Zlib::GzipReader.open(path) do |gz|
              begin
                data = Marshal.load(gz.read)
              ensure
                gz.close
              end
            end
          else
            data = Marshal.load(File.read(path)) rescue {}
          end
        end
      else
        data = {}
      end

      ##
      # Create a weak reference so we can recreate the object later.
      @@data_refs[self.object_id] = self.to_data_ref(data || {})
      @@ref_map[self.object_id] = self
      @data = @@data_refs[self.object_id][:data]
      @@ref_queue.monitor(Ref::WeakReference.new(self))
    end
  end
end
