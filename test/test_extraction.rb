require_relative 'test_case'
require 'csv'

class RI::TestCooccurrencePairGeneration < RI::TestCase

  def test_cofreqs_generation
    known_cofreqs = [
      ["west", "sorcerer"],
      ["west", "sorcerer"],
      ["witch", "east"],
      ["witch", "sorcerer"],
      ["witch", "west"],
      ["witch", "west"]
    ]

    mgr = populate(write_cofreqs: true)
    path = mgr.cofreqs_path()
    assert File.file?(path)
    assert_equal(known_cofreqs.size, File.foreach(path).count)
    cofreqs = CSV.read(path, col_sep: "\t")
    assert_equal(known_cofreqs.sort, cofreqs.sort)
  end

  def test_cofreqs_counts_generation
    known_cofreqs_counts = [
      ["2", "west", "sorcerer"],
      ["1", "witch", "east"],
      ["1", "witch", "sorcerer"],
      ["2", "witch",  "west"]
    ]

    mgr = populate(write_cofreqs: true)
    path = mgr.cofreqs_counts_path()
    assert File.file?(path)
    assert_equal(known_cofreqs_counts.size, File.foreach(path).count)
    
    cofreqs_counts = CSV.read(path, col_sep: "\t")
    # Adjust data format from uniq'd file for easier comparison.
    cofreqs_counts.map { |row| row.first.strip! }
    assert_equal(known_cofreqs_counts.sort, cofreqs_counts.sort)
  end

  def teardown
    mgr = RI::Population::Manager.new(RI::Resource.find("WITCH"), mgrep_client: MockMGREPClient.new)
    settings = mgr.settings
    FileUtils.rm_rf(settings.extraction_output)
  end

  private

  def populate(options = {})
    write_cofreqs = options[:write_cofreqs] == true ? true : false

    res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    mgr = RI::Population::Manager.new(res, {
      mgrep_client: mgrep,
      skip_es_storage: true,
      write_cofreqs: write_cofreqs
    })
    RI.es # triggers delete on teardown
    mgr.populate()
    sleep(2) # wait for indexing to complete

    return mgr
  end
end