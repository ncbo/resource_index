require_relative 'test_case'
require 'csv'

class RI::TestExpansion < RI::TestCase

  def setup
    @converter = RI::Population::LabelConverter.new('localhost', '6379')
    @converter.convert_all
  end

  def test_expansion_valid
    assert File.file?(@converter.expansion_path_sorted())
  end

  def test_expansion_column_count
    csv = CSV.read(@converter.expansion_path_sorted(), col_sep: "\t")
    assert_equal 3, csv.first.size
  end

  def test_expansion_field_types
    csv = CSV.read(@converter.expansion_path_sorted(), col_sep: "\t")
    assert csv.first.all? { |field| field.is_a? String }
  end

  def test_expansion_content
    labels_to_classes = [
      ["EAST", "CCO", "http://purl.obolibrary.org/obo/NCBIGene_46006"],
      ["EAST", "NCIT", "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C45851"],
      ["SORCERER", "ATMO", "http://purl.obolibrary.org/obo/ATM_00010"],
      ["WEST", "NCIT", "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C45852"],
      ["WITCH", "ATMO", "http://purl.obolibrary.org/obo/ATM_00010"],
      ["WITCH", "NCBITAXON", "http://purl.obolibrary.org/obo/NCBITaxon_34819"],
      ["WITCH", "VTO", "http://purl.obolibrary.org/obo/VTO_0055684"]
    ]

    csv = CSV.read(@converter.expansion_path_sorted(), col_sep: "\t")
    assert_equal labels_to_classes.sort, csv
  end

  def teardown
    FileUtils.rm_r([@converter.expansion_path(), @converter.expansion_path_sorted()])
  end

end