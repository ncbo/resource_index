##
# This class is intended to be used as a mixin for LinkedData::Models::Class
# It will provide Resource Index specific functionality
module ResourceIndex
  module Class
    def ihash
      @xxhash ||= RI::IntegerHash.signed_hash(self.submission.ontology.acronym + self.id.to_s)
    end
    alias :xxhash :ihash

    ##
    # Get back counts for a class per resource
    # @resources [Array] Array of strings corresponding to requested resources
    # @opts [Hash] Hash holding options
    #   @opts[:expand] TrueClass|FalseClass
    def ri_counts(*args)
      resources, opts = ri_opts(args)
      counts = {}
      threads = []
      resources.each do |res|
        next unless res.populated?
        threads << Thread.new do
          counts[res.acronym] = res.concept_count(self.xxhash, opts)
        end
      end
      threads.each(&:join)
      counts
    end

    ##
    # Get back counts for a class per resource
    # @resources [Array] Array of strings corresponding to requested resources
    # @opts [Hash] Hash holding options
    #   @opts[:expand] TrueClass|FalseClass
    #   @opts[:size] Integer Size of the result set
    #   @opts[:from] Integer Start at this result
    def ri_docs(*args)
      resources, opts = ri_opts(args)
      raise ArgumentError, "Only one resource allowed" if resources.length > 1
      res = resources.first
      res.concept_docs(self.xxhash, opts)
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