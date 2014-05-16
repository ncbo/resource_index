class RI::Document
  attr_accessor :id, :document_id, :dictionary_id, :resource
  alias :local_element_id :document_id
  alias :"local_element_id=" :"document_id="

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