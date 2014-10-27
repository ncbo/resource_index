require_relative 'test_case'
require 'csv'

class RI::TestCooccurencePairGeneration < RI::TestCase

  def test_label_pair_generation
    known_label_pairs = [
      ["west", "sorcerer"],
      ["west", "sorcerer"],
      ["witch", "east"],
      ["witch", "sorcerer"],
      ["witch", "west"],
      ["witch", "west"]
    ]

    res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(res, mgrep_client: mgrep, bulk_index_size: 500, write_label_pairs: true)
    RI.es # triggers delete on teardown
    index_id = populator.populate()
    sleep(2) # wait for indexing to complete

    label_pairs_path = File.expand_path(File.join("cooccurence_results", "WITCH_labels", "#{index_id}" + ".tsv"))
    assert File.file?(label_pairs_path)
    assert_equal(known_label_pairs.size, File.foreach(label_pairs_path).count)

    label_pairs = CSV.read(label_pairs_path, col_sep: "\t")
    assert_equal(known_label_pairs, label_pairs.sort!)
  end

  def test_class_pair_generation
    known_class_pairs = [
      [1829708204, 560039333],
      [1829708204, 921784164],
      [2135716011, 1829708204],
      [2135716011, 560039333],
      [2135716011, 921784164],
      [2135716011, 921784164],
      [2135716011, 921784164],
      [2471850823, 2135716011],
      [2471850823, 921784164],
      [2957120221, 1829708204],
      [2957120221, 2135716011],
      [2957120221, 2135716011],
      [2957120221, 2135716011],
      [2957120221, 2471850823],
      [2957120221, 560039333],
      [2957120221, 921784164],
      [2957120221, 921784164],
      [2957120221, 921784164],
      [3305416963, 2135716011],
      [3305416963, 2135716011],
      [3305416963, 2471850823],
      [3305416963, 2957120221],
      [3305416963, 2957120221],
      [3305416963, 921784164],
      [3305416963, 921784164],
      [3305416963, 921784164],
      [4174731843, 1829708204],
      [4174731843, 2135716011],
      [4174731843, 2957120221],
      [4174731843, 560039333],
      [4174731843, 921784164],
      [921784164, 560039333]   
    ]

    res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(res, mgrep_client: mgrep, bulk_index_size: 500, write_class_pairs: true)
    RI.es # triggers delete on teardown
    index_id = populator.populate()
    sleep(2) # wait for indexing to complete

    class_pairs_path = File.expand_path(File.join("cooccurence_results", "WITCH_classes", "#{index_id}" + ".tsv"))
    assert File.file?(class_pairs_path)
    assert_equal(known_class_pairs.size, File.foreach(class_pairs_path).count)
  end

  def teardown
    # TODO: Delete label and class pairs files (@index_id is used to generate file names).
    # TODO: Delete entire 'cooccurence_results' directory?
  end
end