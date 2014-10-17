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

CLASSES = {
  4259096489 => ["ACGT-MO", "http://www.ifomis.org/acgt/1.0#Normal"],
  2974317510 => ["RadLex_OWL", "http://bioontology.org/projects/ontologies/radlex/radlexOwl#RID1543"],
  3116223171 => ["SEP", "http://purl.obolibrary.org/obo/sep_00060"],
  30093454   => ["I2B2-LOINC", "http://www.regenstrief.org/loinc#LP28442-9"],
  2631822857 => ["SNOMEDCT", "http://purl.bioontology.org/ontology/SNOMEDCT/17223004"],
  1256174170 => ["EHDA", "http://purl.obolibrary.org/obo/EHDA_3667"],
  3152067627 => ["NCIT", "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C15227"],
  2588657372 => ["PEDTERM", "http://www.owl-ontologies.com/Ontology1358660052.owl#Fetus"],
  2466199616 => ["NCIT", "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C28152"],
  516089123  => ["FMA-SUBSET", "http://purl.obolibrary.org/obo/FMA_74540"],
  1514996459 => ["SNMI", "http://purl.bioontology.org/ontology/SNMI/F-60320"],
  2936938335 => ["NDDF", "http://purl.bioontology.org/ontology/NDDF/010069"],
}

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
    LABEL_ID_TO_CLASS_MAP.values.flatten(1).each do |ids|
      @classes << class_from_ids(ids)
    end
    @classes
  end

  def class_from_ids(ids)
    acronym, cls_id = ids
    ont = MockLinkedDataOntology.new(acronym)
    sub = MockLinkedDataSubmission.new(ont)
    return MockLinkedDataClass.new(cls_id, sub)
  end

end