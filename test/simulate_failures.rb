##
# Monkeypatch to simulate failure when indexing
class RI::Population::Document
  def self.fail_on_index(bool, fail_on_count = 300, max_fails = Float::INFINITY)
    @@fail_count     = 0
    @@failures       = 0
    @@fail_on_count  = fail_on_count
    @@fail_on_index  = bool
    @@fail_count_max = max_fails
  end

  alias_method :old_indexable_hash, :indexable_hash
  def indexable_hash(*args)
    if fail?
      @@failures += 1
      raise RI::Population::Elasticsearch::RetryError
    end
    @@fail_count += 1
    old_indexable_hash(*args)
  end

  def fail?
    @@fail_on_index && @@fail_count > 0 && @@fail_count % @@fail_on_count == 0 && @@failures <= @@fail_count_max
  end
end