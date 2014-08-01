require 'rexml/document'
require 'resource_index/elasticsearch'

module ResourceIndex
  class Resource
    class Field
      attr_accessor :name, :ontology, :weight
      def initialize(name)
        @name = name
      end
    end

    include ResourceIndex::Elasticsearch

    attr_accessor :id, :name, :acronym, :main_field, :homepage, :lookup_url, :description, :logo_url, :count, :updated, :completed, :fields

    def self.all
      @resources ||= RI.db[:obr_resource].all.map {|r| RI::Resource.new(r.values)}
    end

    def self.find(res)
      (Hash[self.all.map {|r| [r.acronym, r]}])[res]
    end

    def initialize(*args)
      cols = args.first.is_a?(Hash) ? args.first.values : args.first
      @id, @name, @acronym, @structure, @main_field, @homepage, @lookup_url, @description, @logo_url, @dict_id, @count, @updated, @completed = *cols
      doc ||= REXML::Document.new(@structure, ignore_whitespace_nodes: :all)
      @fields = {}
      doc.elements.to_a("//contexts/entry/string").each {|a| fields[a.text] = Field.new(a.text.split("_")[1..-1].join("_"))} # Context names, create field obj
      doc.elements.to_a("//weights/entry").map {|a| fields[a.elements["string"].text].weight = a.elements["double"].text.to_f}
      doc.elements.to_a("//ontoIds/entry").select {|a| contain_ont?(a)}.map {|a| fields[a.elements["string[position() = 1]"].text].ontology = a.elements["string[position() = 2]"].text.to_i}
    end

    ##
    # Get fields from structure
    # Include name, related ontologies and weights
    def fields
      @fields
    end

    ##
    # Return a lazy enumerator that will lazily get results from the DB
    def documents(opts = {})
      RI::Document.all(self, opts)
    end

    private

    def contain_ont?(a)
      !a.elements["string[position() = 2]"].text.eql?("null") && !a.elements["string[position() = 2]"].text.eql?("-1")
    end

  end
end