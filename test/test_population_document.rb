require_relative 'test_case'

class RI::TestPopulationDocument < RI::TestCase
  def test_documents
    assert_raises(ArgumentError) {RI::Population::Document.all}
    res = RI::Resource.find("WITCH")
    docs = res.documents(chunk_size: 10)
    assert_equal Enumerator::Lazy, docs.class
    assert_equal 4, docs.to_a.length
    count = 0
    docs.each {|d| count += 1}
    assert_equal 4, count
    known_ids = ["1", "2", "3", "4"]
    ids = docs.map {|d| d.document_id}.force
    assert_equal known_ids.sort, ids.sort
  end

  def test_document_indexable
    res = RI::Resource.find("WITCH")
    doc = res.documents.to_a.first
    hash = doc.indexable_hash
    keys = [:id, :manual_annotations, :witch_class, :witch_sentence]
    assert_equal keys.sort, hash.keys.sort
    assert_equal "wicked witch of the west", hash[:witch_sentence]
  end
end