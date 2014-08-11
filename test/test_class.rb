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
  425808451  => ["SNMI", "http://purl.bioontology.org/ontology/SNMI/F-623C0"],
  4259096489 => ["ACGT-MO", "http://www.ifomis.org/acgt/1.0#Normal"],
  2974317510 => ["RadLex_OWL", "http://bioontology.org/projects/ontologies/radlex/radlexOwl#RID1543"],
  1336324502 => ["NCIT", "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C62590"],
  3116223171 => ["SEP", "http://purl.obolibrary.org/obo/sep_00060"],
  2011060733 => ["PR", "http://purl.obolibrary.org/obo/PR_000004328"],
  30093454   => ["I2B2-LOINC", "http://www.regenstrief.org/loinc#LP28442-9"],
  2631822857 => ["SNOMEDCT", "http://purl.bioontology.org/ontology/SNOMEDCT/17223004"],
  4272574507 => ["SYN", "http://purl.bioontology.org/ontology/MCCL/CL_0000771"],
  1256174170 => ["EHDA", "http://purl.obolibrary.org/obo/EHDA_3667"],
  3152067627 => ["NCIT", "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C15227"],
  2588657372 => ["PEDTERM", "http://www.owl-ontologies.com/Ontology1358660052.owl#Fetus"],
  2783506253 => ["IMR", "http://purl.obolibrary.org/obo/IMR_0200111"],
  1765934245 => ["NCIT", "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C75667"],
  3984536638 => ["PR|SYN", "http://purl.obolibrary.org/obo/PR_000001006"],
  2466199616 => ["NCIT", "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C28152"],
  516089123  => ["FMA-SUBSET", "http://purl.obolibrary.org/obo/FMA_74540"],
  1514996459 => ["SNMI", "http://purl.bioontology.org/ontology/SNMI/F-60320"],
  2936938335 => ["NDDF", "http://purl.bioontology.org/ontology/NDDF/010069"],
  2604095094 => ["NCIT", "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C94894"]
}

DIRECT_COUNT = {2466199616=>2, 3984536638=>4, 2631822857=>1, 4272574507=>1, 1514996459=>2, 2974317510=>22, 2604095094=>1, 425808451=>16, 1336324502=>14, 1256174170=>1, 2783506253=>1, 1765934245=>1, 3152067627=>6, 2588657372=>3, 4259096489=>75, 516089123=>1, 30093454=>1, 3116223171=>12, 2011060733=>58, 2936938335=>1}
DIRECT_DOCS = {2466199616=>["E-GEOD-32422", "E-GEOD-40205"], 3984536638=>["E-GEOD-27302", "E-GEOD-41986", "E-GEOD-42140", "E-GEOD-43307"], 2631822857=>["E-BAIR-1"], 4272574507=>["E-MEXP-3350"], 1514996459=>["E-GEOD-32422", "E-GEOD-43307"], 2974317510=>["E-ERAD-61", "E-GEOD-28471", "E-GEOD-33272", "E-GEOD-33273", "E-GEOD-33341", "E-GEOD-34643", "E-GEOD-34844", "E-GEOD-35179", "E-GEOD-38168", "E-GEOD-39976", "E-GEOD-40366", "E-GEOD-41986", "E-GEOD-42865", "E-GEOD-43177", "E-GEOD-43178", "E-GEOD-43373", "E-GEOD-43484", "E-GEOD-43497", "E-GEOD-43517", "E-GEOD-43553", "E-MTAB-1044", "E-TABM-7"], 2604095094=>["E-GEOD-32422"], 425808451=>["E-GEOD-22087", "E-GEOD-34923", "E-GEOD-37386", "E-GEOD-38110", "E-GEOD-38169", "E-GEOD-38210", "E-GEOD-38211", "E-GEOD-41561", "E-GEOD-42462", "E-GEOD-42551", "E-GEOD-42701", "E-GEOD-42880", "E-GEOD-43287", "E-GEOD-43581", "E-GEOD-43595", "E-MTAB-1371"], 1336324502=>["E-GEOD-35493", "E-GEOD-35843", "E-GEOD-36596", "E-GEOD-37136", "E-GEOD-37842", "E-GEOD-42798", "E-GEOD-42799", "E-GEOD-43324", "E-GEOD-43403", "E-GEOD-43441", "E-GEOD-43464", "E-MEXP-3542", "E-MTAB-1328", "E-SMDB-4088"], 1256174170=>["E-GEOD-39918"], 2783506253=>["E-GEOD-43548"], 1765934245=>["E-GEOD-43465"], 3152067627=>["E-GEOD-26749", "E-GEOD-26750", "E-GEOD-35309", "E-GEOD-39918", "E-GEOD-42912", "E-GEOD-43441"], 2588657372=>["E-GEOD-41331", "E-GEOD-41336", "E-GEOD-43324"], 4259096489=>["E-BAIR-10", "E-BAIR-11", "E-BAIR-12", "E-BAIR-9", "E-ERAD-69", "E-GEOD-13214", "E-GEOD-13561", "E-GEOD-16035", "E-GEOD-23435", "E-GEOD-26600", "E-GEOD-29365", "E-GEOD-29767", "E-GEOD-31030", "E-GEOD-31466", "E-GEOD-32868", "E-GEOD-32934", "E-GEOD-33217", "E-GEOD-33271", "E-GEOD-34119", "E-GEOD-35179", "E-GEOD-35309", "E-GEOD-35493", "E-GEOD-35843", "E-GEOD-37653", "E-GEOD-38228", "E-GEOD-38463", "E-GEOD-38651", "E-GEOD-38756", "E-GEOD-38757", "E-GEOD-38758", "E-GEOD-38759", "E-GEOD-38760", "E-GEOD-38995", "E-GEOD-39159", "E-GEOD-39334", "E-GEOD-39521", "E-GEOD-39998", "E-GEOD-41232", "E-GEOD-41688", "E-GEOD-41776", "E-GEOD-41777", "E-GEOD-42544", "E-GEOD-42701", "E-GEOD-42861", "E-GEOD-42936", "E-GEOD-43282", "E-GEOD-43290", "E-GEOD-43315", "E-GEOD-43324", "E-GEOD-43351", "E-GEOD-43363", "E-GEOD-43407", "E-GEOD-43441", "E-GEOD-43468", "E-GEOD-43469", "E-GEOD-43476", "E-GEOD-43496", "E-GEOD-43548", "E-GEOD-43581", "E-GEOD-43603", "E-GEOD-43621", "E-GEOD-43630", "E-GEOD-43651", "E-LGCL-2", "E-LGCL-3", "E-MEXP-32", "E-MEXP-3657", "E-MIMR-17", "E-MTAB-1044", "E-MTAB-1245", "E-MTAB-1432", "E-SMDB-23", "E-SNGR-9", "E-TIGR-105", "E-TIGR-106"], 516089123=>["E-GEOD-39174"], 30093454=>["E-ERAD-145"], 3116223171=>["E-GEOD-33073", "E-GEOD-33074", "E-GEOD-34643", "E-GEOD-35179", "E-GEOD-37842", "E-GEOD-42865", "E-GEOD-43315", "E-GEOD-43484", "E-GEOD-43630", "E-LGCL-2", "E-LGCL-3", "E-SMDB-3690"], 2011060733=>["E-ERAD-144", "E-ERAD-56", "E-ERAD-59", "E-ERAD-67", "E-ERAD-68", "E-ERAD-92", "E-GEOD-14995", "E-GEOD-15654", "E-GEOD-22087", "E-GEOD-25609", "E-GEOD-26600", "E-GEOD-27237", "E-GEOD-27916", "E-GEOD-31030", "E-GEOD-32868", "E-GEOD-32934", "E-GEOD-34977", "E-GEOD-35039", "E-GEOD-35710", "E-GEOD-36596", "E-GEOD-37136", "E-GEOD-38045", "E-GEOD-38228", "E-GEOD-38756", "E-GEOD-38757", "E-GEOD-38758", "E-GEOD-38759", "E-GEOD-38760", "E-GEOD-38994", "E-GEOD-39150", "E-GEOD-39976", "E-GEOD-40722", "E-GEOD-41232", "E-GEOD-41776", "E-GEOD-41777", "E-GEOD-42259", "E-GEOD-42260", "E-GEOD-42261", "E-GEOD-42880", "E-GEOD-42952", "E-GEOD-43282", "E-GEOD-43300", "E-GEOD-43315", "E-GEOD-43370", "E-GEOD-43410", "E-GEOD-43445", "E-GEOD-43464", "E-GEOD-43471", "E-GEOD-43476", "E-GEOD-43517", "E-GEOD-43535", "E-GEOD-43548", "E-MIMR-7", "E-MTAB-1049", "E-MTAB-1090", "E-MTAB-1217", "E-MTAB-1328", "E-MTAB-840"], 2936938335=>["E-GEOD-43282"]}

class RI::TestQueries < RI::TestCase
  def test_class_xxhash
    CLASSES.each do |hash, ids|
      cls = class_from_ids(ids)
      assert_equal hash, cls.xxhash
    end
  end

  def test_class_queries
    res = RI::Resource.find("AE_test")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(res, mgrep_client: mgrep, bulk_index_size: 500)
    @es = RI.es # triggers delete on teardown
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete

    CLASSES.each do |hash, ids|
      cls = class_from_ids(ids)
      count = cls.ri_counts("AE_test")
      docs = cls.ri_docs("AE_test", size: 500)
      assert_equal DIRECT_COUNT[cls.xxhash], count["AE_test"]
      assert_equal DIRECT_DOCS[cls.xxhash].sort, docs.map {|d| d.id}.sort, "Docs don't match for #{ids.join(' | ')} | #{hash}"
    end
  end

  private

  def class_from_ids(ids)
    acronym, cls_id = ids
    ont = MockLinkedDataOntology.new(acronym)
    sub = MockLinkedDataSubmission.new(ont)
    return MockLinkedDataClass.new(cls_id, sub)
  end

end