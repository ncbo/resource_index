Minitest.after_run do
  # File.open(Dir.pwd + "/converted.dump", 'w') {|f| f.write(Marshal.dump($test_converted)) }
  # File.open(Dir.pwd + "/ancestors.dump", 'w') {|f| f.write(Marshal.dump($test_ancestors)) }
  # File.open(Dir.pwd + "/annotations.dump", 'w') {|f| f.write(Marshal.dump($test_annotations)) }
  # File.open(Dir.pwd + "/annotation_counts.dump", 'w') {|f| f.write(Marshal.dump($test_annotation_counts)) }
  # File.open(Dir.pwd + "/annotation_counts_anc.dump", 'w') {|f| f.write(Marshal.dump($test_annotation_counts_anc)) }
end

##
# Test data for use in populations so we don't need redis, mgrep, 4store, etc
def gzip_read(path)
  data = nil
  Zlib::GzipReader.open(path) do |gz|
    begin
      data = Marshal.load(gz.read)
    ensure
      gz.close
    end
  end
  data
end
$test_converted = gzip_read(File.expand_path("../data/converted.dump.gz", __FILE__))
$test_annotations = gzip_read(File.expand_path("../data/annotations.dump.gz", __FILE__))
$test_annotation_counts = gzip_read(File.expand_path("../data/annotation_counts.dump.gz", __FILE__))
$test_annotation_counts_anc = gzip_read(File.expand_path("../data/annotation_counts_anc.dump.gz", __FILE__))
$test_ancestors = gzip_read(File.expand_path("../data/ancestors.dump.gz", __FILE__))
$test_latest_sub = gzip_read(File.expand_path("../data/latest_sub.dump.gz", __FILE__))

##
# Monkeypatched mock methods for use in populations so we don't need redis, mgrep, 4store, etc
class RI::Population::LabelConverter
  def convert(mgrep_matches)
    $test_converted[mgrep_matches]
  end
end

module MockMGREP
  def annotate(text,longword,wholeword=true)
    text = text.force_encoding('UTF-8').upcase.gsub("\n"," ")
    $test_annotations[text]
  end
end

class RI::Population::Mgrep::Client
  include MockMGREP
end

class MockMGREPClient
  include MockMGREP
end

class RI::Population::Class
  def retrieve_ancestors(acronym, submission_id)
    $test_ancestors[self.xxhash]
  end
end

class RI::Population::Manager
  def latest_submissions_sparql
    $test_latest_sub
  end
end

##
# Monkeypatch to simulate failure when indexing
class RI::Document
  def self.fail_on_index(bool, fail_on_count = 300, max_fails = Float::INFINITY)
    @@fail_count     = 0
    @@failures       = 0
    @@fail_on_count = fail_on_count
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