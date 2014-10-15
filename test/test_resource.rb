require_relative 'test_case'

class RI::TestResouce < RI::TestCase
  def test_resource
    res = RI::Resource.find("PGDR")
    assert_equal "PGDR", res.acronym
    assert_equal "PharmGKB [Drug]", res.name
    res = RI::Resource.find("AE")
    assert_equal "AE", res.acronym
    assert_equal "ArrayExpress", res.name
  end

  def test_resource_fields
    res = RI::Resource.find("GM")
    assert_equal ["GM_caption", "GM_title"].sort, res.fields.keys.sort
    assert_equal ["caption", "title"].sort, res.fields.values.map {|f| f.name}
    assert_equal [0.8, 1.0].sort, res.fields.values.map {|f| f.weight}.sort
    assert res.fields.values.all? {|f| f.is_a?(RI::Resource::Field)}
    res = RI::Resource.find("AE")
    assert_equal ["AE_name", "AE_description", "AE_species", "AE_experiment_type"].sort, res.fields.keys.sort
    assert_equal ["name", "description", "species", "experiment_type"].sort, res.fields.values.map {|f| f.name}.sort
    assert_equal [1.0, 0.8, 1.0, 0.9].sort, res.fields.values.map {|f| f.weight}.sort
    assert res.fields.values.all? {|f| f.is_a?(RI::Resource::Field)}
  end

  def test_new_resource
    a = RI::Resource.new
    assert_equal RI::Resource, a.class
    b = RI::Resource.new({name: "Test Resource", acronym: "TR"})
    assert_equal "Test Resource", b.name
    assert_equal "TR", b.acronym
    c = RI::Resource.new(b)
    assert_equal c.object_id, c.object_id
  end

  def test_resources
    res = RI::Resource.all
    assert_equal 11, res.length
    known_resource_ids = ["AE", "CT", "GM", "OMIM", "CDD", "PGDI", "PGDR", "PGGE", "REAC", "UPKB", "WITCH"].sort
    known_names = [
      "ArrayExpress",
      "ClinicalTrials.gov",
      "ARRS GoldMiner",
      "Online Mendelian Inheritance in Man",
      "Conserved Domain Database (CDD)",
      "PharmGKB [Disease]",
      "PharmGKB [Drug]",
      "PharmGKB [Gene]",
      "Reactome",
      "UniProt KB",
      "Wicked Witch"
    ].sort
    resource_ids = res.map {|r| r.acronym}.sort
    names = res.map {|r| r.name}.sort
    assert_equal known_resource_ids, resource_ids
    assert_equal known_names, names
  end
end