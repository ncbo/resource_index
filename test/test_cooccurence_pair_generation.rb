require_relative 'test_case'

class RI::TestCooccurencePairGeneration < RI::TestCase

  @class_pairs = [
    [1829708204, 560039333],
    [1829708204, 921784164],
    [2135716011, 1829708204],
    [2135716011, 560039333],
    [2135716011, 921784164],
    [2135716011, 921784164],
    [2135716011, 921784164],
    [2957120221, 1829708204],
    [2957120221, 2135716011],
    [2957120221, 2135716011],
    [2957120221, 2135716011],
    [2957120221, 560039333],
    [2957120221, 921784164],
    [2957120221, 921784164],
    [2957120221, 921784164],
    [3305416963, 2135716011],
    [3305416963, 2135716011],
    [3305416963, 2957120221],
    [3305416963, 2957120221],
    [3305416963, 921784164],
    [3305416963, 921784164],
    [3305416963, 921784164],
    [921784164, 560039333]
  ]

  @label_pairs = [
    ["west", "sorcerer"],
    ["west", "sorcerer"],
    ["witch", "east"],
    ["witch", "sorcerer"],
    ["witch", "west"],
    ["witch", "west"]
  ]

  def test_pair_generation
    res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(res, 
    {
      mgrep_client: mgrep, 
      bulk_index_size: 500, 
      write_label_pairs: true, 
      write_class_pairs: true
    })
    @es = RI.es # triggers delete on teardown
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete
  end

  def teardown
    # TODO: Delete label and class pairs files (@index_id is used to generate file names).
    # TODO: Delete entire 'cooccurence_results' directory?
  end
end