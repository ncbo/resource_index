require 'set'
require_relative 'test_case'

HASHED_CLASSES = Hash[CLASSES.map {|c| [c[:xxhash], c]}]

class RI::TestPopulationClass < RI::TestCase
  def test_class_hash
    HASHED_CLASSES.each do |hash, data|
      cls = class_from_ids([data[:id], data[:ontology]])
      assert_equal hash, cls.xxhash
      assert_equal hash, cls.hash
    end
  end

  def test_class_equivalence
    set = Set.new
    hash = Hash.new
    LABEL_ID_TO_CLASS_MAP.values.each do |class_hash|
      class_hash.each do |class_data|
        a = class_from_ids(class_data)
        b = class_from_ids(class_data)
        assert a == b
        assert a.eql?(b)
        assert b == a
        assert b.eql?(a)
        set << a
        set << b
        hash[a] = true
        hash[b] = false
      end
    end
    assert set.length == classes().length
    assert hash.length == classes().length
    assert hash.all? {|k,v| v == false}
  end

  private

  def classes
    return @classes if @classes
    @classes = Set.new
    LABEL_ID_TO_CLASS_MAP.values.each do |classes_hash|
      classes_hash.each do |class_data|
        @classes << class_from_ids(class_data)
      end
    end
    @classes
  end

  def class_from_ids(cls)
    cls_id = cls[0]
    acronym = cls[1]
    return RI::Population::Class.new(cls_id, acronym)
  end

end