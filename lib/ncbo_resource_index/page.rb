require 'goo'

class ResourceIndex::Page < Goo::Base::Page
  def initialize(docs, resource, opts)
    total = docs["hits"]["total"] rescue binding.pry
    return super(0, 0, 0, []) if total == 0
    limit = opts[:size]
    offset = opts[:from]
    current_page = (total / limit) - ((total - offset) / limit) + 1
    super(current_page, limit, total, docs["hits"]["hits"].map {|doc| RI::Document.from_elasticsearch(doc, resource)})
  end
end