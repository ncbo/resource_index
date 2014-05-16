require 'rexml/document'

module ResourceIndex
  class Resource
    class Field
      attr_accessor :name, :ontology, :weight
      def initialize(name)
        @name = name
      end
    end

    def self.all
      res = RI.db[:obr_resource]
      res.all.map {|r| RI::Resource.new(r.values)}
    end

    def self.find(res)
      res = RI.db[:obr_resource].where(resource_id: res).limit(1).first
      RI::Resource.new(res)
    end

    attr_accessor :id, :name, :acronym, :main_field, :homepage, :lookup_url, :description, :logo_url, :count, :updated, :completed, :fields
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
      unless RI::Document.const_defined?(self.acronym)
        fields = self.fields.keys.map {|f| f.downcase.to_sym}
        cls = Class.new(RI::Document) do
          fields.each do |field|
            define_method field do
              instance_variable_get("@#{field}")
            end
            define_method "#{field}=".to_sym do |arg|
              instance_variable_set("@#{field}", arg)
            end
          end
        end
        cls.define_singleton_method :from_hash do |hsh|
          inst = self.new
          hsh.each {|k,v| inst.send("#{k}=", v)}
          inst
        end
        RI::Document.const_set(self.acronym, cls)
      end
      cls ||= RI::Document.const_get(self.acronym)
      return Enumerator.new { |yielder|
        offset = 0
        docs = nil
        while docs.nil? || docs.length > 0
          docs = RI.db["obr_#{self.acronym.downcase}_element".to_sym].limit(chunk_size).offset(offset).all
          docs.each do |doc|
            doc[:resource] = self.acronym
            yielder << cls.from_hash(doc) if doc
          end
          offset += chunk_size
        end
      }.lazy
    end
  end
end