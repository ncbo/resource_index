require 'rexml/document'

module ResourceIndex
  class Resource
    class Field
      attr_accessor :name, :ontology, :weight
      def initialize(name)
        @name = name
      end
    end

    attr_accessor :id, :name, :acronym, :main_field, :homepage, :lookup_url, :description, :logo_url, :count, :updated, :completed, :fields

    def self.all
      @@all ||= RI.db[:obr_resource].all.map {|r| RI::Resource.new(r.values)}
    end

    def self.find(res)
      @@all_hash ||= Hash[self.all.map {|r| [r.acronym, r]}]
      @@all_hash[res]
    end

    def initialize(*args)
      cols = args.first.is_a?(Hash) ? args.first.values : args.first
      @id, @name, @acronym, @structure, @main_field, @homepage, @lookup_url, @description, @logo_url, @dict_id, @count, @updated, @completed = *cols
      doc ||= REXML::Document.new(@structure, ignore_whitespace_nodes: :all) rescue binding.pry
      @fields = {}
      doc.elements.to_a("//contexts/entry/string").each {|a| fields[a.text] = Field.new(a.text.split("_")[1..-1].join("_"))} # Context names, create field obj
      doc.elements.to_a("//weights/entry").map {|a| fields[a.elements["string"].text].weight = a.elements["double"].text.to_f}
      doc.elements.to_a("//ontoIds/entry").select {|a| !a.elements["string[position() = 2]"].text.eql?("null")}.map {|a| fields[a.elements["string[position() = 1]"].text].ontology = a.elements["string[position() = 2]"].text.to_i}
    end

    ##
    # Get fields from structure
    # Include name, related ontologies and weights
    def fields
      @fields
    end

    ##
    # Return a lazy enumerator that will lazily get results from the DB
    def documents(chunk_size: 5000)
      RI::Document.all(resource: self, chunk_size: chunk_size)
    end
  end
end