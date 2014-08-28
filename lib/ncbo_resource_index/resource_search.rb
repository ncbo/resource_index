require 'ncbo_resource_index/elasticsearch'

module ResourceIndex::ResourceSearch
  include ResourceIndex::Elasticsearch

  def concept_count(hash, opts = {})
    es_concept_count(hash, opts)
  end

  def concept_docs(hash, opts = {})
    es_concept_docs(hash, opts).map {|doc| RI::Document.from_elasticsearch(doc, self)}
  end

  def concept_docs_page(hash, opts = {})
    docs = es_doc(hash, opts)
    RI::Page.new(docs, self, opts)
  end
end