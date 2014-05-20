class RI::Document
  attr_accessor :id, :document_id, :dictionary_id, :resource
  alias :local_element_id :document_id
  alias :"local_element_id=" :"document_id="

  ##
  # Return a lazy enumerator that will lazily get results from the DB
  def self.all(resource: nil, chunk_size: 5000)
    raise ArgumentError, "Please provide a resource" unless resource.is_a?(RI::Resource)
    unless RI::Document.const_defined?(resource.acronym)
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
    end
    cls ||= RI::Document.const_get(resource.acronym)
    return Enumerator.new { |yielder|
      offset = 0
      docs = nil
      while docs.nil? || docs.length > 0
        docs = RI.db["obr_#{resource.acronym.downcase}_element".to_sym].limit(chunk_size).offset(offset).all
        docs.each do |doc|
          doc[:resource] = resource.acronym
          yielder << cls.from_hash(doc) if doc
        end
        offset += chunk_size
      end
    }.lazy
  end

  def indexable_hash
    fields = RI::Resource.find(self.resource).fields.keys.map {|f| f.downcase.to_sym}
    hash = {}
    fields.each {|f| hash[f] = self.send(f)}
    hash
  end

  def annotatable_text
    fields = RI::Resource.find(self.resource).fields.keys.map {|f| f.downcase.to_sym}
    fields.map {|f| self.send(f)}.join("\n\n")
  end
end