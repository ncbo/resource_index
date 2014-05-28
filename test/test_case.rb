require 'pry'
require 'minitest/autorun'
require_relative '../lib/resource_index'

module RI
  class TestCase < Minitest::Test

  end
end

Minitest.after_run do
  # File.open(Dir.pwd + "/converted.dump", 'w') {|f| f.write(Marshal.dump($converted)) }
  # File.open(Dir.pwd + "/ancestors.dump", 'w') {|f| f.write(Marshal.dump($hierarchies)) }
  # File.open(Dir.pwd + "/annotations.dump", 'w') {|f| f.write(Marshal.dump($annotations)) }
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
$test_ancestors = gzip_read(File.expand_path("../data/ancestors.dump.gz", __FILE__))
$test_latest_sub = gzip_read(File.expand_path("../data/latest_sub.dump.gz", __FILE__))

##
# Monkeypatched mock methods for use in populations so we don't need redis, mgrep, 4store, etc
class RI::Population::LabelConverter
  def convert(mgrep_matches)
    $test_converted[mgrep_matches]
  end
end

class Annotator::Mgrep::Client
  def annotate(text,longword,wholeword=true)
    text = text.force_encoding('UTF-8').upcase.gsub("\n"," ")
    anns = $test_annotations[text]
    binding.pry unless anns
    anns
  end
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