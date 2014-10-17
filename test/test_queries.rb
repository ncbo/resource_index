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
    CLASS_XXHASH.each do |id|
      assert_equal DIRECT_ANNOTATION_COUNTS[id], res.concept_count(id), "direct count with id #{id} is bad"
    end

    XXHASH_TO_ANCESTOR_XXHASH.values.flatten.uniq.each do |id|
      assert_equal ANCESTOR_ANNOTATION_COUNTS[id], res.concept_count(id, expand: true), "ancestor count with id #{id} is bad"
    end

    ##
    # Test returned documents
    CLASS_XXHASH.each do |id|
      docs = res.concept_docs(id, size: 500).map {|d| d.id}.sort
      assert_equal XXHASH_TO_DOCS[id].sort, docs
    end

    XXHASH_TO_ANCESTOR_XXHASH.values.flatten.uniq.each do |id|
      docs = res.concept_docs(id, expand: true, size: 500).map {|d| d.id}.sort
      assert_equal ANCESTOR_XXHASH_TO_DOCS[id].sort, docs, "In local but not ES: #{ANCESTOR_XXHASH_TO_DOCS[id].sort - docs}\nIn ES but not local: #{docs - ANCESTOR_XXHASH_TO_DOCS[id].sort}"
    end

    ##
    # Test multiple classes in a single doc (via AND)
    hashes = [560039333, 1829708204]
    docs = res.es_concept_docs(hashes)
    assert_equal 1, docs.length
    assert_equal "2", docs.first["_id"]

    ##
    # Test multiple classes in a single doc (via OR)
    hashes = [3305416963, 921784164]
    docs = res.es_concept_docs(hashes, bool: :should)
    assert_equal 4, docs.length
    assert_equal ["1", "2", "3", "4"].sort, docs.map {|d| d["_id"]}.sort

    ##
    # Pass bad options to count query (should still work)
    hashes = [560039333, 1829708204]
    count = res.es_concept_count(hashes, size: 50, from: 0)
    assert_equal 1, count
  end
end