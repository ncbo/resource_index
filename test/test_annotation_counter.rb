require_relative 'test_case'

class RI::TestQueries < RI::TestCase
  def test_counts
    res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(res, mgrep_client: mgrep, bulk_index_size: 500)
    @es = RI.es # triggers delete on teardown
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete

    @es.indices.delete index: :counts rescue puts "No counts index"

    counts = RI.counts
    assert_equal({direct: 74455182918, ancestors: 135753841736}, counts[:total])

    ##
    # Test counts
    ac = RI::Population::AnnotationCounter.new()
    ac.count_and_store
    sleep(1)
    ac.count_and_store
    sleep(1)
    ac.count_and_store
    sleep(1)

    counts = RI.counts

    results = @es.search index: :counts, body: {}
    assert_equal(3, results["hits"]["total"])
    time_sorted = results["hits"]["hits"].sort {|a, b| Time.parse(a["_source"]["time"]) <=> Time.parse(b["_source"]["time"])}
    assert_equal(time_sorted.last["_source"]["time"], counts["time"])
    assert_equal(17, counts["total"]["direct"])
    assert_equal(195, counts["total"]["ancestors"])
  end
end