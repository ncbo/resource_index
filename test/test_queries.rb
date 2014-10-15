require_relative 'test_case'

class RI::TestQueries < RI::TestCase
  def test_query
    res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(res, mgrep_client: mgrep, bulk_index_size: 500)
    @es = RI.es # triggers delete on teardown
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete

    ##
    # Test counts
    DIRECT.each do |id|
      assert_equal DIRECT_COUNT[id], res.concept_count(id), "direct count with id #{id} is bad"
    end

    ANCESTOR.each do |id|
      assert_equal ANCESTOR_COUNT[id], res.concept_count(id, expand: true), "ancestor count with id #{id} is bad"
    end

    ##
    # Test returned documents
    DIRECT.each do |id|
      docs = res.concept_docs(id, size: 500).map {|d| d.id}.sort
      assert_equal DIRECT_DOCS[id].sort, docs
    end

    ANCESTOR.each do |id|
      docs = res.concept_docs(id, expand: true, size: 500).map {|d| d.id}.sort
      assert_equal ANCESTOR_DOCS[id].sort, docs, "In local but not ES: #{ANCESTOR_DOCS[id].sort - docs}\nIn ES but not local: #{docs - ANCESTOR_DOCS[id].sort}"
    end

    ##
    # Test multiple classes in a single doc (via AND)
    hashes = [1514996459, 2466199616]
    docs = res.es_concept_docs(hashes)
    assert_equal 1, docs.length
    assert_equal "E-GEOD-32422", docs.first["_id"]

    ##
    # Test multiple classes in a single doc (via OR)
    hashes = [2466199616, 2631822857]
    docs = res.es_concept_docs(hashes, bool: :should)
    assert_equal 3, docs.length
    assert_equal ["E-GEOD-32422", "E-GEOD-40205", "E-BAIR-1"].sort, docs.map {|d| d["_id"]}.sort

    ##
    # Pass bad options to count query (should still work)
    hashes = [1514996459, 2466199616]
    count = res.es_concept_count(hashes, size: 50, from: 0)
    assert_equal 1, count
  end
end