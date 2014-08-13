module ResourceIndex
  class Document
    attr_accessor :documentId
    alias :id :documentId
    alias :id= :documentId=

    def self.find(resource, doc_id)
      resource = resource.is_a?(String) ? Resource.find(resource) : resource
      raise ArgumentError, "Must provide ResourceIndex::Resource object, not #{resource.class.name}" unless resource.is_a?(ResourceIndex::Resource)
      raise ArgumentError, "Invalid resource #{res_id}" unless resource
      record = ResourceIndex.es.get(index: resource.current_index_id, id: doc_id)
      from_elasticsearch(record, resource)
    end

    def self.fields
      @fields
    end

    def self.from_elasticsearch(record, resource)
      raise ArgumentError, "Must provide resource object, not #{resource.class}" unless resource.is_a?(ResourceIndex::Resource)
      record = record.dup
      record["id"] = record.delete("_id")
      ["_index", "_type", "_score", "_version", "found"].each {|i| record.delete(i)}
      record.merge!(record.delete("_source") || {})
      doc_subclass(resource).from_hash(record)
    end

    def self.doc_subclass(resource)
      if RI::Document.const_defined?(resource.acronym)
        return RI::Document.const_get(resource.acronym)
      end

      fields = resource.fields.keys.map {|f| f.to_sym}
      cls = ::Class.new(RI::Document) do
        @fields = fields
        @fields.each do |field|
          define_method field do
            instance_variable_get("@#{field}")
          end
          define_method "#{field}=".to_sym do |arg|
            instance_variable_set("@#{field}", arg)
          end
          # The population process downcases everything before putting
          # it into Elasticsearch, this maintaings compatability
          define_method field.downcase do
            instance_variable_get("@#{field}")
          end
          define_method "#{field.downcase}=".to_sym do |arg|
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

    def to_hash
      Hash[self.instance_variables.map {|var| [var[1..-1], self.instance_variable_get(var)] }]
    end

  end
end