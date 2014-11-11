require_relative 'test_case'
require 'csv'

class RI::TestCooccurrencePairGeneration < RI::TestCase

  def test_label_pair_generation
    known_label_pairs = [
      ["west", "sorcerer"],
      ["west", "sorcerer"],
      ["witch", "east"],
      ["witch", "sorcerer"],
      ["witch", "west"],
      ["witch", "west"]
    ]

    mgr = populate(write_label_pairs: true)
    label_pairs_path = mgr.label_pairs_path
    assert File.file?(label_pairs_path)
    assert_equal(known_label_pairs.size, File.foreach(label_pairs_path).count)
    label_pairs = CSV.read(label_pairs_path, col_sep: "\t")
    assert_equal(known_label_pairs.sort, label_pairs.sort)
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

    mgr = populate(write_class_pairs: true)
    
    # Ensure proper files are generated.
    class_pairs_path = mgr.class_pairs_path
    assert File.file?(class_pairs_path)
    decryption_path = mgr.decryption_path
    assert File.file?(decryption_path)
    
    assert_equal(known_class_pairs.size, File.foreach(class_pairs_path).count)
    
    class_pairs = CSV.read(class_pairs_path, col_sep: "\t")
    assert_equal(known_class_pairs.sort, class_pairs.sort)
  end

  def teardown
    mgr = RI::Population::Manager.new(RI::Resource.find("WITCH"), mgrep_client: MockMGREPClient.new)
    settings = mgr.settings
    FileUtils.rm_rf(settings.cooccurrence_output)
  end

  private

  def populate(options = {})
    write_class_pairs = options[:write_class_pairs] == true ? true : false
    write_label_pairs = options[:write_label_pairs] == true ? true : false

    res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    mgr = RI::Population::Manager.new(res, {
      mgrep_client: mgrep,
      skip_es_storage: true,
      write_class_pairs: write_class_pairs,
      write_label_pairs: write_label_pairs
    })
    RI.es # triggers delete on teardown
    index_id = mgr.populate()
    sleep(2) # wait for indexing to complete

    return mgr
  end
end