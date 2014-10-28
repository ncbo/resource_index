require_relative 'test_case'
require 'csv'

class RI::TestCooccurencePairGeneration < RI::TestCase

  COOCCURENCE_RESULTS_DIR = 'cooccurence_results'

  def test_label_pair_generation
    known_label_pairs = [
      ["west", "sorcerer"],
      ["west", "sorcerer"],
      ["witch", "east"],
      ["witch", "sorcerer"],
      ["witch", "west"],
      ["witch", "west"]
    ]

    index_id = populate(write_label_pairs: true)
    label_pairs_path = File.expand_path(File.join(COOCCURENCE_RESULTS_DIR, "WITCH_labels", "#{index_id}" + ".tsv"))
    assert File.file?(label_pairs_path)
    assert_equal(known_label_pairs.size, File.foreach(label_pairs_path).count)

    label_pairs = CSV.read(label_pairs_path, col_sep: "\t")
    assert_equal(known_label_pairs, label_pairs.sort!)
  end

  def test_class_pair_generation
    known_class_pairs = [
      ["1829708204", "560039333"],
      ["1829708204", "921784164"],
      ["1971434104", "921784164"],
      ["2135716011", "1829708204"],
      ["2135716011", "1971434104"],
      ["2135716011", "560039333"],
      ["2135716011", "921784164"],
      ["2135716011", "921784164"],
      ["2135716011", "921784164"],
      ["2957120221", "1829708204"],
      ["2957120221", "1971434104"],
      ["2957120221", "2135716011"],
      ["2957120221", "2135716011"],
      ["2957120221", "2135716011"],
      ["2957120221", "560039333"],
      ["2957120221", "921784164"],
      ["2957120221", "921784164"],
      ["2957120221", "921784164"],
      ["3305416963", "1971434104"],
      ["3305416963", "2135716011"],
      ["3305416963", "2135716011"],
      ["3305416963", "2957120221"],
      ["3305416963", "2957120221"],
      ["3305416963", "921784164"],
      ["3305416963", "921784164"],
      ["3305416963", "921784164"],
      ["3317937907", "1829708204"],
      ["3317937907", "2135716011"],
      ["3317937907", "2957120221"],
      ["3317937907", "560039333"],
      ["3317937907", "921784164"],
      ["921784164", "560039333"]   
    ]

    index_id = populate(write_class_pairs: true)
    class_pairs_path = File.expand_path(File.join(COOCCURENCE_RESULTS_DIR, "WITCH_classes", "#{index_id}" + ".tsv"))
    assert File.file?(class_pairs_path)
    assert_equal(known_class_pairs.size, File.foreach(class_pairs_path).count)

    class_pairs = CSV.read(class_pairs_path, col_sep: "\t")
    assert_equal(known_class_pairs, class_pairs.sort!)
  end

  def teardown
    FileUtils.rm_rf(COOCCURENCE_RESULTS_DIR)
  end

  private

  def populate(options = {})
    write_class_pairs = options[:write_class_pairs] == true ? true : false
    write_label_pairs = options[:write_label_pairs] == true ? true : false

    res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(res, {
      mgrep_client: mgrep, 
      bulk_index_size: 500, 
      write_class_pairs: write_class_pairs,
      write_label_pairs: write_label_pairs
    })
    RI.es # triggers delete on teardown
    index_id = populator.populate()
    sleep(2) # wait for indexing to complete

    return index_id
  end
end