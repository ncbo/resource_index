require 'set'
require_relative 'test_case'

HASHED_CLASSES = {
  2135716011 => ["NCBITAXON", "http://purl.obolibrary.org/obo/NCBITaxon_34819"],
  2957120221 => ["VTO", "http://purl.obolibrary.org/obo/VTO_0055684"],
  921784164  => ["ATMO", "http://purl.obolibrary.org/obo/ATM_00010"],
  3305416963 => ["NCIT", "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C45852"],
  560039333  => ["CCO", "http://purl.obolibrary.org/obo/NCBIGene_46006"],
  1829708204 => ["NCIT", "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C45851"]
}

class RI::TestPopulationClass < RI::TestCase
  def test_class_hash
    HASHED_CLASSES.each do |hash, ids|
      cls = class_from_ids(ids)
      assert_equal hash, cls.xxhash
      assert_equal hash, cls.hash
    end
  end

  def test_class_equivalence
    set = Set.new
    hash = Hash.new
    LABEL_ID_TO_CLASS_MAP.values.flatten(1).each do |ids|
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
    assert set.length == classes().length
    assert hash.length == classes().length
    assert hash.all? {|k,v| v == false}
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
    return RI::Population::Class.new(cls_id, acronym)
  end

end