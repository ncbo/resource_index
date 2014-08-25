require 'rexml/document'
require 'ncbo_resource_index/elasticsearch'

module ResourceIndex
  class Resource
    class Field
      attr_accessor :name, :ontology, :weight
      def initialize(name)
        @name = name
      end

      def to_hash
        {name: @name, ontology: @ontology, weight: @weight}
      end
    end

    include ResourceIndex::Elasticsearch

    attr_accessor :id, :name, :acronym, :mainField, :homepage, :lookupURL, :description, :logo, :count, :updated, :completed, :fields
    alias :logo_url :logo
    alias :logo_url= :logo=
    alias :lookup_url :lookupURL
    alias :lookup_url= :lookupURL=
    alias :main_field :mainField
    alias :main_field= :mainField=

    def self.all
      @resources ||= lazy_resources_in_es
    end

    def self.populated
      self.all.select {|r| r.populated?}
    end

    def self.find(res)
      (Hash[self.all.map {|r| [r.acronym, r]}])[res]
    end

    def initialize(*args)
      return args.first if args.first.is_a?(Resource)
      cols = args.first.is_a?(Hash) ? args.first.values : args.first
      @id, @name, @acronym, @structure, @mainField, @homepage, @lookupURL, @description, @logo, @dict_id, @count, @updated, @completed = *cols
      doc ||= REXML::Document.new(@structure, ignore_whitespace_nodes: :all)
      @fields = {}
      doc.elements.to_a("//contexts/entry/string").each {|a| fields[a.text] = Field.new(a.text.split("_")[1..-1].join("_"))} # Context names, create field obj
      doc.elements.to_a("//weights/entry").map {|a| fields[a.elements["string"].text].weight = a.elements["double"].text.to_f}
      doc.elements.to_a("//ontoIds/entry").select {|a| contain_ont?(a)}.map {|a| fields[a.elements["string[position() = 1]"].text].ontology = RI::VIRT_MAP[a.elements["string[position() = 2]"].text.to_i]}
    end

    ##
    # Search for the current index id, not the alias
    # If we find more than one, error (that's bad)
    def current_index_id
      idx = ResourceIndex.es.indices.get_aliases.select {|index_id,index_meta| index_meta["aliases"].key?(@acronym)}.map {|index_id,index_meta| index_id}
      raise ArgumentError, "More than one index found for #{@acronym}" if idx.length > 1
      raise ArgumentError, "No index found for #{@acronym}" if idx.length == 0
      return idx.first
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
      RI::Population::Document.all(self, opts)
    end

    ##
    # Returns whether or not this resource is populated in the index
    def populated?
      RI.es.indices.exists_alias name: @acronym
    end

    def to_hash
      Hash[self.instance_variables.map {|var| [var[1..-1], self.instance_variable_get(var)] }]
    end

    private

    def contain_ont?(a)
      !a.elements["string[position() = 2]"].text.eql?("null") && !a.elements["string[position() = 2]"].text.eql?("-1")
    end

    ##
    # Get the resources from ES if they are available.
    # If not, get them from the configured database and store them.
    # If the resources in ES are older than a week, update them.
    def self.lazy_resources_in_es
      resource_store = RI.settings[:resource_store]
      resources = RI.es.get(index: resource_store, id: "resources")["_source"] rescue nil
      if resources.nil? || old?(resources)
        resources = RI.db[:obr_resource].all.map {|r| RI::Resource.new(r.values)}.sort {|a,b| a.name.downcase <=> b.name.downcase}
        if resources.nil? || resources.empty?
          raise StandardError, "No resources found in SQL DB"
        else
          resources = {"time" => Time.now.to_f, "resources" => resources.map {|r| r.to_hash}}
          RI.es.index index: resource_store, type: "resources", id: "resources", body: resources
        end
      end
      raise StandardError, "No resources found" if resources["resources"].nil?
      resources["resources"].map {|r| Resource.new(r)}
    end

    def self.old?(resources)
      resources && resources[:time] && (Time.at(resources[:time]).to_date < Time.now.to_date - 7)
    end

  end
end