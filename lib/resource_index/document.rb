class RI::Document
  attr_accessor :id, :document_id, :dictionary_id, :resource
  alias :local_element_id :document_id
  alias :"local_element_id=" :"document_id="

  ##
  # Return a lazy enumerator that will lazily get results from the DB
  def self.all(resource, opts = {}, mutex = nil)
    raise ArgumentError, "Please provide a resource" unless resource.is_a?(RI::Resource)
    chunk_size = opts[:chunk_size] || 5000
    record_limit = opts[:record_limit] || Float::INFINITY
    mutex ||= Mutex.new
    cls = nil
    mutex.synchronize {
      unless RI::Document.const_defined?(resource.acronym)
        cls = create_doc_subclass(resource)
      end
    }
    cls ||= RI::Document.const_get(resource.acronym)
    return Enumerator.new { |yielder|
      offset = opts[:offset] || 0
      docs = nil
      record_count = 0
      while (docs.nil? || docs.length > 0) && record_count < record_limit
        docs = RI.db["obr_#{resource.acronym.downcase}_element".to_sym].limit(chunk_size).offset(offset).all
        docs.each do |doc|
          doc[:resource] = resource.acronym
          yielder << cls.from_hash(doc) if doc
        end
        offset += chunk_size
        record_count += chunk_size
      end
    }.lazy
  end

  def self.count(resource)
    RI.db["obr_#{resource.acronym.downcase}_element".to_sym].count
  end

  def self.threach(resource, opts = {}, mutex = nil, &block)
    thread_count = opts[:thread_count] || 1
    threads = []
    chunk_size = (self.count(resource).to_f / thread_count).ceil
    opts = opts.dup
    opts[:record_limit] = chunk_size
    mutex ||= Mutex.new
    thread_count.times do |i|
      threads << Thread.new do
        opts = opts.dup
        opts[:offset] = chunk_size * i
        self.all(resource, opts, mutex).each do |doc|
          yield doc if block_given?
        end
      end
    end
    threads.each(&:join)
  end

  def indexable_hash
    fields = RI::Resource.find(self.resource).fields.keys.map {|f| f.downcase.to_sym}
    hash = {}
    fields.each {|f| hash[f] = self.send(f).force_encoding('UTF-8')}
    hash[:id] = self.document_id
    hash
  end

  def annotatable_text
    fields = RI::Resource.find(self.resource).fields.keys.map {|f| f.downcase.to_sym}
    fields.map {|f| self.send(f)}.join("\n\n")
  end

  private

  def self.create_doc_subclass(resource)
    fields = resource.fields.keys.map {|f| f.downcase.to_sym}
    cls = Class.new(RI::Document) do
      fields.each do |field|
        define_method field do
          instance_variable_get("@#{field}")
        end
        define_method "#{field}=".to_sym do |arg|
          instance_variable_set("@#{field}", arg)
        end
      end
    end
    cls.define_singleton_method :from_hash do |hsh|
      inst = self.new
      hsh.each {|k,v| inst.send("#{k}=", v)}
      inst
    end
    RI::Document.const_set(resource.acronym, cls)
    cls
  end
end