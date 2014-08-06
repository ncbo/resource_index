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
    manual_annotations_ok?
  end

  def test_population_threaded
    @res = RI::Resource.find("AE_test")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, bulk_index_size: 500, population_threads: 2)
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete
    docs_ok?
    population_ok?
    manual_annotations_ok?
  end

  def test_population_resume
    RI::Population::Document.fail_on_index(true)
    @es = Elasticsearch::Client.new
    @res = RI::Resource.find("AE_test")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, bulk_index_size: 500)
    @index_id = populator.index_id
    assert_raises RI::Population::Elasticsearch::RetryError do
      populator.populate()
    end
    sleep(3)
    assert Dir.glob(Dir.pwd + "/ae_test*resume").length > 0
    RI::Population::Document.fail_on_index(false)
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, bulk_index_size: 500)
    assert_equal ({}), @es.indices.get_alias(index: populator.index_id, name: "error")
    @index_id = populator.populate()
    sleep(3) # wait for indexing to complete
    docs_ok?
    population_ok?
    manual_annotations_ok?
    assert Dir.glob(Dir.pwd + "/#{@index_id}*resume").empty?
  end

  def test_population_resume_threaded
    RI::Population::Document.fail_on_index(true)
    @es = Elasticsearch::Client.new
    @res = RI::Resource.find("AE_test")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, bulk_index_size: 500, population_threads: 2)
    @index_id = populator.index_id
    assert_raises RI::Population::Elasticsearch::RetryError do
      populator.populate()
    end
    sleep(3)
    assert Dir.glob(Dir.pwd + "/ae_test*resume").length > 0
    RI::Population::Document.fail_on_index(false)
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, bulk_index_size: 500, population_threads: 2)
    assert_equal ({}), @es.indices.get_alias(index: populator.index_id, name: "error")
    @index_id = populator.populate()
    sleep(3) # wait for indexing to complete
    docs_ok?
    population_ok?
    manual_annotations_ok?
    assert Dir.glob(Dir.pwd + "/#{@index_id}*resume").empty?
  end

  def test_population_no_resume
    @res = RI::Resource.find("AE_test")
    mgrep = MockMGREPClient.new
    RI::Population::Document.fail_on_index(true, 1, 6)
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, bulk_index_size: 500, resume: false)
    @index_id = populator.index_id
    assert_raises RI::Population::Elasticsearch::RetryError do
      populator.populate()
    end
    assert_equal 0, Dir.glob(Dir.pwd + "/ae_test*resume").length
  end

  def test_population_manual_resume
    @res = RI::Resource.find("AE_test")
    mgrep = MockMGREPClient.new
    RI::Population::Document.fail_on_index(true)
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, bulk_index_size: 500, resume: false)
    @index_id = populator.index_id
    assert_raises RI::Population::Elasticsearch::RetryError do
      populator.populate()
    end
    sleep(3)
    RI::Population::Document.fail_on_index(false)
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, bulk_index_size: 500, starting_offset: 300, time_int: @index_id.split("_").last.to_i)
    @index_id = populator.populate()
    sleep(3) # wait for indexing to complete
    docs_ok?
    population_ok?
    manual_annotations_ok?
  end

  def test_population_recover
    @es = Elasticsearch::Client.new
    @res = RI::Resource.find("AE_test")
    mgrep = MockMGREPClient.new
    RI::Population::Document.fail_on_index(true, 5, 7)
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, bulk_index_size: 500, resume: false)
    @index_id = populator.index_id
    retry_count = -1
    assert_raises RI::Population::Elasticsearch::RetryError do
      begin
        populator.populate()
      rescue RI::Population::Elasticsearch::RetryError => e
        retry_count = e.retry_count
        raise e
      end
    end
    assert_equal 5, retry_count
    sleep(3)
    @es.indices.delete index: @index_id
    RI::Population::Document.fail_on_index(true, 5, 2)
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, bulk_index_size: 500)
    @index_id = populator.populate()
    RI::Population::Document.fail_on_index(false)
    sleep(3) # wait for indexing to complete
    docs_ok?
    population_ok?
    manual_annotations_ok?
  end

  def manual_annotations_ok?
    @es = Elasticsearch::Client.new
    require 'pp'
    MANUAL_ANNOTATION_XXHASH.each do |hash|
      es_count = @es.count index: @index_id, body: direct_query(hash)
      assert_equal MANUAL_ANNOTATION_COUNTS[hash], es_count["count"].to_i
    end
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
    assert_equal TOTAL_ES_RECORDS, stats["_all"]["primaries"]["docs"]["count"]
    count = @es.count index: @index_id
    assert_equal 464, count["count"]
    aliased_id = @es.indices.get_alias(name: "AE_test").keys.first
    assert_equal @index_id, aliased_id
    # We shuffle to get random counts here, doing all takes 100 seconds
    $test_annotation_counts.keys.shuffle[0..100].each do |direct|
      ann_count = $test_annotation_counts[direct]
      es_count = @es.count index: @index_id, body: direct_query(direct)
      assert_equal ann_count, es_count["count"]
    end
    $test_annotation_counts_anc.keys.shuffle[0..100].each do |anc|
      ann_count = $test_annotation_counts_anc[anc]
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