module Enumerable
  WRONG_TYPE_ERROR = "Using ResourceIndex monkeypatches requires all elements be of type ResourceIndex::Class"

  ##
  # Get back counts for a class per resource
  # @resources [Array] Array of strings corresponding to requested resources
  # @opts [Hash] Hash holding options
  #   @opts[:expand] TrueClass|FalseClass
  def ri_counts(*args)
    raise ArgumentError, WRONG_TYPE_ERROR unless self.all? {|e| e.is_a?(ResourceIndex::Class)}
    hashes = self.map {|c| c.xxhash}
    resources, opts = ri_opts(args)
    counts = {}
    threads = []
    resources.each do |res|
      next unless res.populated?
      threads << Thread.new do
        counts[res.acronym] = res.concept_count(hashes, opts)
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
    raise ArgumentError, WRONG_TYPE_ERROR unless self.all? {|e| e.is_a?(ResourceIndex::Class)}
    hashes = self.map {|c| c.xxhash}
    resources, opts = ri_opts(args)
    raise ArgumentError, "Only one resource allowed" if resources.length > 1
    res = resources.first
    res.concept_docs(hashes, opts)
  end

  ##
  # Get back counts for a class per resource
  # @resources [Array] Array of strings corresponding to requested resources
  # @opts [Hash] Hash holding options
  #   @opts[:expand] TrueClass|FalseClass
  #   @opts[:size] Integer Size of the result set
  #   @opts[:from] Integer Start at this result
  def ri_docs_page(*args)
    raise ArgumentError, WRONG_TYPE_ERROR unless self.all? {|e| e.is_a?(ResourceIndex::Class)}
    hashes = self.map {|c| c.xxhash}
    resources, opts = ri_opts(args)
    raise ArgumentError, "Only one resource allowed" if resources.length > 1
    res = resources.first
    res.concept_docs_page(hashes, opts)
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