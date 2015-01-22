require 'set'
require_relative 'test_case'

class MockLinkedDataClass
  include RI::Class
  attr_accessor :id, :submission
  def initialize(id, submission)
    @id = id; @submission = submission
  end
end

class MockLinkedDataSubmission
  attr_accessor :ontology
  def initialize(ontology)
    @ontology = ontology
  end
end

class MockLinkedDataOntology
  attr_accessor :acronym
  def initialize(acronym)
    @acronym = acronym
  end
end

class RI::TestClass < RI::TestCase
  def test_class_xxhash
    hashes = CLASS_XXHASH.dup
    hashes.each do |hash|
      assert classes().any? {|c| c.xxhash == hash}, "Can't find hash #{hash}"
    end
  end

  def test_class_queries
    res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(res, mgrep_client: mgrep, bulk_index_size: 500)
    @es = RI.es # triggers delete on teardown
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete

    classes().each do |cls|
      count = cls.ri_counts("WITCH")
      docs = cls.ri_docs("WITCH", size: 500)
      assert_equal XXHASH_TO_DOCS[cls.xxhash].length, count["WITCH"]
      assert_equal XXHASH_TO_DOCS[cls.xxhash].sort, docs.map {|d| d.id}.sort, "Docs don't match for #{cls}"
    end
  end

  private

  def classes
    return @classes if @classes
    @classes = Set.new
    LABEL_ID_TO_CLASS_MAP.values.each do |classes_hash|
      classes_hash.each do |class_data|
        @classes << class_from_ids(class_data)
      end
    end
    @classes
  end

  def class_from_ids(ids)
    cls_id = ids.first
    acronym = ids.last.split('/').last
    ont = MockLinkedDataOntology.new(acronym)
    sub = MockLinkedDataSubmission.new(ont)
    return MockLinkedDataClass.new(cls_id, sub)
  end

end