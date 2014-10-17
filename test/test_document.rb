require 'set'
require_relative 'test_case'

MORE_THAN_ONE_DOC = Set.new([2135716011, 2957120221, 921784164, 3305416963])

class RI::TestDocument < RI::TestCase

  def test_documents
    res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(res, mgrep_client: mgrep)
    @es = RI.es # triggers delete on teardown
    @index_id = populator.populate()
    sleep(1) # wait for indexing to complete

    found = []
    CLASS_XXHASH.each do |hash|
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

      # For classes that have more than one doc, we will try to page through to find all of them
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
    assert_equal 9, found.length
    assert_equal 0, MORE_THAN_ONE_DOC.length
  end

  def test_document_find
    res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    doc_id = RI.db[:obr_witch_element][id: 1][:local_element_id]
    populator = RI::Population::Manager.new(res, mgrep_client: mgrep)
    @es = RI.es # triggers delete on teardown
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete

    assert_raises(ArgumentError) do
      ResourceIndex::Document.find({}, doc_id)
    end
    assert_raises(ArgumentError) do
      ResourceIndex::Document.find("AE_doesnt_exist", doc_id)
    end

    doc = ResourceIndex::Document.find("WITCH", doc_id)
    name = "wicked witch of the west"
    assert_equal name, doc.witch_sentence, "Bad witch_sentence"
    cls = ["NCBITAXON\u001Fhttp://purl.obolibrary.org/obo/NCBITaxon_10090"]
    assert_equal cls, doc.witch_class, "Bad witch_class"
  end

  private

  def offset_for_page(page, pagesize)
    page * pagesize - pagesize
  end

end