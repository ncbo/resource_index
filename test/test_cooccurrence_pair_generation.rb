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

  def test_cooccurrence_counts_generation
    known_counts = [
      ["2", "west", "sorcerer"],
      ["1", "witch", "east"],
      ["1", "witch", "sorcerer"],
      ["2", "witch",  "west"]
    ]

    mgr = populate(write_label_pairs: true)
    cooccurrence_path = mgr.cooccurrence_counts_path
    assert File.file?(cooccurrence_path)
    assert_equal(known_counts.size, File.foreach(cooccurrence_path).count)
    
    cooccurrence_counts = CSV.read(cooccurrence_path, col_sep: "\t")
    # Adjust data format from uniq'd file for easier comparison.
    cooccurrence_counts.each do |row|
      string = row.first
      row.unshift(string[3])
      row[1] = string[5, string.size]
    end
    assert_equal(known_counts.sort, cooccurrence_counts.sort)
  end

  def teardown
    mgr = RI::Population::Manager.new(RI::Resource.find("WITCH"), mgrep_client: MockMGREPClient.new)
    settings = mgr.settings
    FileUtils.rm_rf(settings.cooccurrence_output)
  end

  private

  def populate(options = {})
    write_label_pairs = options[:write_label_pairs] == true ? true : false

    res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    mgr = RI::Population::Manager.new(res, {
      mgrep_client: mgrep,
      skip_es_storage: true,
      write_label_pairs: write_label_pairs
    })
    RI.es # triggers delete on teardown
    mgr.populate()
    sleep(2) # wait for indexing to complete

    return mgr
  end
end