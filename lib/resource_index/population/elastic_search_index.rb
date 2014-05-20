module RI::Population
  class ElasticSearchIndex
    attr_reader :name, :type

    def initialize(es_url)
      @es_url = es_url
    end

    def create
    end

    def alias
    end

  end
end
