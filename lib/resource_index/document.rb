module ResourceIndex
  class Document
    attr_accessor :id, :resource

    def self.fields
      @fields
    end

    def self.from_elasticsearch(record, resource)
      record = record.dup
      record["resource"] = resource.acronym
      record["id"] = record.delete("_id")
      ["_index", "_type", "_score"].each {|i| record.delete(i)}
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