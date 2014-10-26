class RI::Population::Document
  attr_accessor :id, :document_id, :dictionary_id, :resource
  alias :local_element_id :document_id
  alias :"local_element_id=" :"document_id="

  ##
  # Return a lazy enumerator that will lazily get results from the DB
  def self.all(resource, opts = {}, mutex = nil)
    raise ArgumentError, "Please provide a resource" unless resource.is_a?(RI::Resource)
    chunk_size = opts[:chunk_size] || 5000
    record_limit = opts[:record_limit] || Float::INFINITY
    chunk_size = record_limit if chunk_size > record_limit
    mutex ||= Mutex.new
    cls = nil
    mutex.synchronize {
      unless RI::Population::Document.const_defined?(resource.acronym)
        cls = create_doc_subclass(resource)
      end
    }
    cls ||= RI::Population::Document.const_get(resource.acronym)
    return Enumerator.new { |yielder|
      offset = opts[:offset] || 0
      docs = nil
      record_count = 0
      while (docs.nil? || docs.length > 0) && record_count < record_limit
        docs = RI.db["obr_#{resource.acronym.downcase}_element".to_sym].limit(chunk_size).offset(offset).all
        docs.each do |doc|
          doc[:resource] = resource
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
    opts[:offset] ||= 0
    threads = []
    thread_limit = ((self.count(resource) - opts[:offset]).to_f / thread_count).ceil
    opts = opts.dup
    opts[:record_limit] = thread_limit
    mutex ||= Mutex.new
    thread_count.times do |i|
      threads << Thread.new do
        new_opts = opts.dup
        new_opts[:offset] = opts[:offset] + (thread_limit * i)
        self.all(resource, new_opts, mutex).each do |doc|
          yield doc if block_given?
        end
      end
    end
    threads.each(&:join)
  end

  def indexable_hash
    fields = self.resource.fields.keys.map {|f| f.downcase.to_sym}
    hash = {}
    fields.each {|f| hash[f] = self.send(f).force_encoding('UTF-8')}
    ont_fields = self.resource.fields.lazy.select {|f| f[1].ontology}.map {|f| f[0].to_sym}
    hash[:manual_annotations] = []
    # Look up manual annotations from the old ids
    ont_fields.each do |f|
      f = f.downcase
      next if hash[f].nil? || hash[f].empty?
      ids = hash[f].split("> ")
      hash[f] = []
      ids.each do |id|
        ont, cls = id.split("/")
        ont = clean_ont_id(ont)
        cls = clean_cls_id(ont, cls)

        onts = RI.db.from(:obs_ontology)
        begin
          local_ont_id = onts[virtual_ontology_id: ont][:local_ontology_id]
        rescue => e
          puts "Manual annotations, problem getting ontology #{ont}: #{e.message}"
          next
        end

        concepts = RI.db.from(:obs_concept)
        begin
          cls_uri = concepts.where(local_concept_id: "#{local_ont_id}/#{cls}").first[:full_id]
        rescue => e
          puts "Manual annotations, problem getting concept #{ont} | #{local_ont_id}/#{cls}: #{e.message}"
          next
        end

        acronym = RI::VIRT_MAP[ont.to_i].upcase
        cls = RI::Population::Class.new(cls_uri, acronym)
        hash[f] << "#{acronym}\C-_#{cls_uri}"
        hash[:manual_annotations] << cls
      end
    end
    hash[:id] = self.document_id
    hash
  end

  def annotatable_text
    fields = self.resource.fields.keys.map {|f| f.downcase.to_sym}
    fields.map {|f| self.send(f)}.join("\n\n")
  end

  private

  def clean_cls_id(ont, cls)
    case ont.to_i
    when 1132
      cls = "obo:#{cls.sub(':', '_')}" unless cls.start_with?("obo:")
    when 1070

    end
    cls
  end

  def clean_ont_id(ont)
    case ont.to_i
    when 46440
      ont = 1070
    end
    ont
  end

  def self.create_doc_subclass(resource)
    # TODO: would be good to change this to not downcase (would mess with existing index, only do when reindexing everything)
    fields = resource.fields.keys.map {|f| f.downcase.to_sym}
    cls = Class.new(RI::Population::Document) do
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
    RI::Population::Document.const_set(resource.acronym, cls)
    cls
  end
end