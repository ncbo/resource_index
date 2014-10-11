require 'set'
require_relative 'test_case'

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

class RI::TestPopulationClass < RI::TestCase
  def test_class_hash
    CLASSES.each do |hash, ids|
      cls = class_from_ids(ids)
      assert_equal hash, cls.xxhash
      assert_equal hash, cls.hash
    end
  end

  def test_class_equivalence
    set = Set.new
    hash = Hash.new
    CLASSES.values.each do |ids|
      a = class_from_ids(ids)
      b = class_from_ids(ids)
      assert a == b
      assert a.eql?(b)
      assert b == a
      assert b.eql?(a)
      set << a
      set << b
      hash[a] = true
      hash[b] = false
    end
    assert set.length == CLASSES.length
    assert hash.length == CLASSES.length
    assert hash.all? {|k,v| v == false}
  end

  private

  def class_from_ids(ids)
    acronym, cls_id = ids
    return RI::Population::Class.new(cls_id, acronym)
  end

end