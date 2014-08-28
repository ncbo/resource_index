require 'ncbo_resource_index/elasticsearch'

class ResourceIndex::MultiSearch
  include ResourceIndex::Elasticsearch

  def concept_docs(hash, opts)
    page = opts.delete(:page)
    resources = {}
    RI::Resource.populated.each {|r| resources[r.current_index_id] = r}
    docs = es_concept_docs_multi(resources.keys, hash, opts)
    resources_docs = {}
    docs["responses"].each do |res_docs|
      matches = res_docs["hits"]["hits"]
      next unless matches && matches.length > 0
      resource = resources[matches.first["_index"]]
      if page
        ri_docs = RI::Page.new(res_docs, resource, opts)
      else
        ri_docs = matches.map {|doc| RI::Document.from_elasticsearch(doc, resource)}
      end
      resources_docs[resource.acronym] = ri_docs
    end
    resources_docs
  end

end