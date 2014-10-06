require 'elasticsearch'

module RI::Population::Elasticsearch

  def index_id
    "#{@res.acronym.downcase}_#{@time.to_i}"
  end

  def create_index
    return if @es.indices.exists index: index_id
    @es.indices.create index: index_id, type: "#{@res.acronym.downcase}_doc", body: es_mapping
    @es.indices.put_alias index: index_id, name: "#{@res.acronym}_populating"
  end

  def alias_index
    previous = (@es.indices.get_alias name: @res.acronym).keys.first rescue nil # get the prior index
    @es.indices.put_alias index: previous, name: "#{@res.acronym}_previous" if previous # add RES_previous alias for easy rollback
    old_aliases = @es.indices.get_aliases.select {|k,v| v["aliases"].key?(@res.acronym)} # list of anything else with the alias (safety check)
    old_aliases.each {|k,v| @es.indices.delete_alias index: k, name: @res.acronym} # delete the old stuff
    @es.indices.put_alias index: index_id, name: @res.acronym # name new index
    @es.indices.delete_alias index: index_id, name: "#{@res.acronym}_populating" # remove populating
  end

  def alias_error
    @es.indices.put_alias index: index_id, name: "error"
  end

  def delete_unaliased
    indices = @es.indices.get_aliases index: "#{@res.acronym.downcase}*"
    indices.each {|index_id, hsh| @es.indices.delete index: index_id if hsh["aliases"].empty? || (hsh["aliases"].key?("error"))}
  end

  def remove_error_alias
    begin
      @es.indices.delete_alias index: index_id, name: "error"
    rescue Elasticsearch::Transport::Transport::Errors::NotFound
      # Alias not found, move on
    end
  end

  def es_mapping
    properties_json = Hash[@res.fields.keys.map {|f| [f.downcase.to_sym, {type: :string}] }]
    {
      mappings: {
        :"#{@res.acronym.downcase}_doc" => {
          :"_source" => {
            includes: @res.fields.keys.map {|f| f.downcase.to_sym},
            excludes: [:annotations]
          },
          properties: properties_json.merge(ANNOTATIONS_MAPPING)
        }
      }
    }
  end

ANNOTATIONS_MAPPING = {
  annotations: {
      type: :nested,
      properties: {
          direct: {type: :long},
          ancestors: {type: :long},
          count: {type: :long}
      }
  }
}
end
