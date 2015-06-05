require_relative 'test_case'

class RI::TestDocument < RI::TestCase
  def test_population
    @res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep)
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete
    population_ok?
    docs_ok?
    manual_annotations_ok?
  end

  def test_population_threaded
    @res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, population_threads: 2)
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete
    population_ok?
    docs_ok?
    manual_annotations_ok?
  end

  def test_population_skip
    @res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, skip_es_storage: true)
    @index_id = populator.populate()
    begin
      index_exists = RI.es.indices.exists(index: @index_id)
    rescue Faraday::TimeoutError
      es_timeout = true
    end
    assert es_timeout || !index_exists
  end

  def test_population_resume
    RI::Population::Document.fail_on_index(true)
    @es = Elasticsearch::Client.new
    @res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep)
    @index_id = populator.index_id
    assert_raises RI::Population::Indexing::RetryError do
      populator.populate()
    end
    sleep(1)
    assert File.exist?(populator.resume_path)
    RI::Population::Document.fail_on_index(false)
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep)
    assert_equal ({}), @es.indices.get_alias(index: populator.index_id, name: "error")
    @index_id = populator.populate()
    sleep(1) # wait for indexing to complete
    population_ok?
    docs_ok?
    manual_annotations_ok?
    assert !File.exist?(populator.resume_path)
  end

  def test_population_resume_threaded
    RI::Population::Document.fail_on_index(true)
    @es = Elasticsearch::Client.new
    @res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, population_threads: 2)
    @index_id = populator.index_id
    assert_raises RI::Population::Indexing::RetryError do
      populator.populate()
    end
    sleep(1)
    assert File.exist?(populator.resume_path)
    RI::Population::Document.fail_on_index(false)
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, population_threads: 2)
    assert_equal ({}), @es.indices.get_alias(index: populator.index_id, name: "error")
    @index_id = populator.populate()
    sleep(1) # wait for indexing to complete
    population_ok?
    docs_ok?
    manual_annotations_ok?
    assert !File.exist?(populator.resume_path)
  end

  def test_population_no_resume
    @res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    RI::Population::Document.fail_on_index(true, 1, 6)
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, resume: false)
    @index_id = populator.index_id
    assert_raises RI::Population::Indexing::RetryError do
      populator.populate()
    end
    assert_equal 0, Dir.glob(Dir.pwd + "/witch*resume").length
  end

  def test_population_manual_resume
    @res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    RI::Population::Document.fail_on_index(true, 3)
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, resume: false)
    @index_id = populator.index_id
    assert_raises RI::Population::Indexing::RetryError do
      populator.populate()
    end
    sleep(1)
    RI::Population::Document.fail_on_index(false)
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, starting_offset: 3, time_int: @index_id.split("_").last.to_i)
    @index_id = populator.populate()
    sleep(1) # wait for indexing to complete
    population_ok?
    docs_ok?
    manual_annotations_ok?
  end

  def test_population_recover
    @es = Elasticsearch::Client.new
    @res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    RI::Population::Document.fail_on_index(true, 1, 7)
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep, resume: false)
    @index_id = populator.index_id
    retry_count = -1
    assert_raises RI::Population::Indexing::RetryError do
      begin
        populator.populate()
      rescue RI::Population::Indexing::RetryError => e
        retry_count = e.retry_count
        raise e
      end
    end
    assert_equal 5, retry_count
    sleep(1)
    @es.indices.delete index: @index_id
    RI::Population::Document.fail_on_index(true, 5, 2)
    populator = RI::Population::Manager.new(@res, mgrep_client: mgrep)
    @index_id = populator.populate()
    RI::Population::Document.fail_on_index(false)
    sleep(1) # wait for indexing to complete
    population_ok?
    docs_ok?
    manual_annotations_ok?
  end

  def manual_annotations_ok?
    @es = Elasticsearch::Client.new
    MANUAL_ANNOTATION_CLASSES.each do |acr_id|
      cls = RI::Population::Class.new(acr_id[1], acr_id[0])
      es_count = @es.count index: @index_id, body: direct_query(cls.hash)
      assert_equal 1, es_count["count"].to_i, "Manual annotation failed for #{cls.hash}"
    end
  end

  def docs_ok?
    @es = Elasticsearch::Client.new
    docs = @res.documents(record_limit: 100)
    docs.each do |doc|
      es_doc = @es.get(index: @index_id, id: doc.document_id)
      es_doc["_source"].each do |field, value|
        assert_equal value, doc.indexable_hash[field.to_sym], "Doc retrieval failed for #{doc}"
      end
    end
  end

  def population_ok?
    @es = Elasticsearch::Client.new
    stats = @es.indices.stats index: @index_id
    assert_equal TOTAL_ES_RECORDS, stats["_all"]["primaries"]["docs"]["count"]
    count = @es.count index: @index_id
    assert_equal 4, count["count"]
    aliased_id = @es.indices.get_alias(name: "WITCH").keys.first
    assert_equal @index_id, aliased_id
    CLASS_XXHASH.each do |direct|
      ann_count = DIRECT_ANNOTATION_COUNTS[direct]
      es_count = @es.count index: @index_id, body: direct_query(direct)
      assert_equal ann_count, es_count["count"], "Bad direct counts for #{direct}"
    end
    XXHASH_TO_ANCESTOR_XXHASH.values.flatten.uniq.each do |anc|
      ann_count = ANCESTOR_ANNOTATION_COUNTS[anc]
      es_count = @es.count index: @index_id, body: ancestor_query(anc)
      assert_equal ann_count, es_count["count"], "Bad ancestor counts for #{anc}"
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