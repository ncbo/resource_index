require_relative 'test_case'

class RI::TestDocument < RI::TestCase
  def test_population
    @res = RI::Resource.find("AE_test")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, bulk_index_size: 500)
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete
    docs_ok?
    population_ok?
  end

  def test_population_threaded
    @res = RI::Resource.find("AE_test")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, bulk_index_size: 500, population_threads: 2)
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete
    docs_ok?
    population_ok?
  end

  def test_population_resume
    RI::Document.fail_on_index(true)
    @es = Elasticsearch::Client.new
    @res = RI::Resource.find("AE_test")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, bulk_index_size: 500)
    assert_raises StandardError do
      populator.populate()
    end
    sleep(3)
    RI::Document.fail_on_index(false)
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, bulk_index_size: 500)
    assert_equal ({}), @es.indices.get_alias(index: populator.index_id, name: "error")
    @index_id = populator.populate()
    sleep(3) # wait for indexing to complete
    docs_ok?
    population_ok?
    assert Dir.glob(Dir.pwd + "/#{@index_id}*resume").empty?
  end

  def docs_ok?
    @es = Elasticsearch::Client.new
    docs = @res.documents(record_limit: 100)
    docs.each do |doc|
      es_doc = @es.get(index: @index_id, id: doc.document_id)
      es_doc["_source"].each do |field, value|
        assert_equal value, doc.indexable_hash[field.to_sym]
      end
    end
  end

  def population_ok?
    @es = Elasticsearch::Client.new
    stats = @es.indices.stats index: @index_id
    assert_equal 961193, stats["_all"]["primaries"]["docs"]["count"]
    count = @es.count index: @index_id
    assert_equal 464, count["count"]
    aliased_id = @es.indices.get_alias(name: "AE_test").keys.first
    assert_equal @index_id, aliased_id
    $test_annotation_counts.each do |direct, ann_count|
      es_count = @es.count index: @index_id, body: direct_query(direct)
      assert_equal ann_count, es_count["count"]
    end
    $test_annotation_counts_anc.each do |anc, ann_count|
      es_count = @es.count index: @index_id, body: ancestor_query(anc)
      assert_equal ann_count, es_count["count"]
    end
  end

  def direct_query(concept)
    {
       query: {
          nested: {
             path: "annotations",
             query: {
                match: {
                   :"annotations.direct" => concept
                }
             }
          }
       }
    }
  end

  def ancestor_query(ancestor)
    {
       query: {
          nested: {
             path: "annotations",
             query: {
                match: {
                   :"annotations.ancestors" => ancestor
                }
             }
          }
       }
    }
  end
end