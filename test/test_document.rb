require 'set'
require_relative 'test_case'

DIRECT = [2466199616, 3984536638, 2631822857, 4272574507, 1514996459, 2974317510, 2604095094, 425808451, 1336324502, 1256174170, 2783506253, 1765934245, 3152067627, 2588657372, 4259096489, 516089123, 30093454, 3116223171, 2011060733, 2936938335]
MORE_THAN_ONE_DOC = Set.new([2466199616, 3984536638, 1514996459, 2974317510, 425808451, 1336324502, 3152067627, 2588657372, 4259096489, 3116223171, 2011060733])

class RI::TestDocument < RI::TestCase

  def test_documents
    res = RI::Resource.find("AE_test")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(res, mgrep_client: mgrep, bulk_index_size: 500)
    @es = RI.es # triggers delete on teardown
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete

    found = []
    DIRECT.each do |hash|
      es_docs = res.es_concept_docs(hash)
      docs = res.concept_docs(hash)
      docs.each_with_index do |doc, i|
        assert_equal es_docs[i]["_id"], doc.id
        fields = es_docs[i]["_source"]
        fields.each do |k, v|
          assert doc.respond_to?(k)
          assert_equal v, doc.send(k)
        end
      end

      deleted = MORE_THAN_ONE_DOC.delete?(hash)
      if deleted
        page = res.concept_docs_page(hash, size: 1)
        next_page = page.next_page
        assert page.total_pages > 0
        while next_page
          found.concat(page)
          assert page.length == 1
          page = res.concept_docs_page(hash, size: 1, from: offset_for_page(next_page, 1))
          next_page = page.next_page
        end
      end
    end
    assert_equal 203, found.length
    assert_equal Set.new, MORE_THAN_ONE_DOC
  end

  private

  def offset_for_page(page, pagesize)
    page * pagesize - pagesize
  end

end