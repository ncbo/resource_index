module ResourceIndex
  class Document
    attr_accessor :id, :resource

    def initialize
      unless RI::Document.const_defined?(resource.acronym)
        cls = create_doc_subclass(resource)
      end
      cls ||= RI::Document.const_get(resource.acronym)
    end

    def self.from_elasticsearch(record)
      record[:id] = record.delete(:_id)
      [:_index, :_type, :_score].each {|i| record.delete(i)}
      record.merge(record.delete(_source))
      self.from_hash(record)
    end

    def self.create_doc_subclass(resource)
      fields = resource.fields.keys.map {|f| f.to_sym}
      cls = Class.new(RI::Document) do
        fields.each do |field|
          define_method field do
            instance_variable_get("@#{field}")
          end
          define_method "#{field}=".to_sym do |arg|
            instance_variable_set("@#{field}", arg)
          end
          # The population process downcases everything before putting
          # it into Elasticsearch, this maintaings compatability
          down_field = field.downcase.to_sym
          down_field_set = "#{down_field}=".to_sym
          field_set = "#{field}=".to_sym
          alias down_field field
          alias down_field_set field_set
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
end