require 'rexml/document'
require 'ncbo_resource_index/resource_search'
require 'ncbo_resource_index/images'

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

    include ResourceIndex::ResourceSearch

    attr_accessor :id, :name, :acronym, :mainField, :homepage, :lookupURL, :description, :logo, :count, :updated, :completed, :fields
    alias :logo_url :logo
    alias :logo_url= :logo=
    alias :lookup_url :lookupURL
    alias :lookup_url= :lookupURL=
    alias :main_field :mainField
    alias :main_field= :mainField=

    OLD_RI_ATTR_MAP = {
      resource_id: :acronym,
      main_context: :mainField,
      url: :homepage,
      element_url: :lookupURL,
      dictionary_id: :dict_id,
      total_element: :count,
      last_update_date: :updated,
      workflow_completed_date: :completed
    }.freeze

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
      return if args.empty? # allow instantiation of new resources without data
      return args.first if args.first.is_a?(Resource)
      raise ArgumentError, "Need to pass a hash to initialize a resource" unless args.first.is_a?(Hash)

      resource = args.first
      resource.each do |attr, value|
        attr = OLD_RI_ATTR_MAP[attr.to_sym] || attr
        instance_variable_set("@#{attr}", value)
      end

      @logo = Images::URI[@acronym] # replace URL with image URI that can be used in HTML <img> elements

      # Convert fields from database
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
      idx = ResourceIndex.es.indices.get_aliases.select {|index_id,index_meta| index_meta["aliases"] && index_meta["aliases"].key?(@acronym) rescue binding.pry}.map {|index_id,index_meta| index_id}
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

      es_available = true
      begin
        resources = RI.es.get(index: resource_store, id: "resources")["_source"]
      rescue Faraday::TimeoutError
        es_available = false
      rescue ::Elasticsearch::Transport::Transport::Errors::NotFound
        es_available = false
      end

      if resources.nil? || old?(resources)
        resources = RI.db[:obr_resource].all.map {|r| RI::Resource.new(r)}.sort {|a,b| a.name.downcase <=> b.name.downcase}
        if resources.nil? || resources.empty?
          raise StandardError, "No resources found in SQL DB"
        else
          resources = {"time" => Time.now.to_f, "resources" => resources.map {|r| r.to_hash}}
          RI.es.index index: resource_store, type: "resources", id: "resources", body: resources if es_available
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