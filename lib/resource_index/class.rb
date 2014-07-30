##
# This class is intended to be used as a mixin for LinkedData::Models::Class
# It will provide Resource Index specific functionality
module ResourceIndex
  module Class
    def xxhash
      @xxhash ||= XXhash.xxh32(self.submission.ontology.acronym + self.id.to_s, RI::HASH_SEED)
    end

    ##
    # Get back counts for a class per resource
    # @resources [Array] Array of strings corresponding to requested resources
    # @opts [Hash] Hash holding options
    #   @opts[:expand] TrueClass|FalseClass
    def ri_counts(*args)
      resources, opts = ri_opts(args)
      counts = {}
      resources.each do |res|
        counts[res.acronym] = res.concept_count(self.xxhash, expand: opts[:expand])
      end
      counts
    end

    ##
    # Get back counts for a class per resource
    # @resources [Array] Array of strings corresponding to requested resources
    # @opts [Hash] Hash holding options
    #   @opts[:expand] TrueClass|FalseClass
    #   @opts[:size] Integer Size of the result set
    def ri_docs(*args)
      resources, opts = ri_opts(args)
      docs = {}
      resources.each do |res|
        docs[res.acronym] = res.concept_docs(self.xxhash, expand: opts[:expand], size: opts[:size])
      end
      docs
    end

    private

    def ri_opts(args)
      opts = args.pop
      if !opts.is_a?(Hash)
        args.push(opts)
        opts = {}
      end
      resources = args.map {|res| RI::Resource.find(res.to_s)}.compact
      resources = ResourceIndex::Resource.all if resources.empty?
      return resources, opts
    end
  end
end