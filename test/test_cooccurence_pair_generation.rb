require_relative 'test_case'

class RI::TestCooccurencePairGeneration < RI::TestCase
  def test_pair_generation
    skip 'Add tests when process for generating pairs is finalized'

    res = RI::Resource.find("AE_test")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(res, mgrep_client: mgrep, bulk_index_size: 500, write_label_pairs: true, write_class_pairs: true)
    @es = RI.es # triggers delete on teardown
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete

    binding.pry
  end
end