class RI::Document
  attr_accessor :id, :document_id, :dictionary_id, :resource
  alias :local_element_id :document_id
  alias :"local_element_id=" :"document_id="
end