##
# Running this file requires access to a full working NCBO stack, including:
# - mgrep
# - annotator redis cache
# - 4store
# Locations are configured in the test_data method
##

$skip_population_setup = true
$skip_prevent_persist = true

require_relative 'test_case'

## Global data store
$test_converted             = Hash.new
$test_annotations           = Hash.new
$test_ancestors             = Hash.new
$test_latest_sub            = Hash.new
$test_annotation_counts     = Hash.new
$test_annotation_counts_anc = Hash.new

##
# Monkeypatched mock methods for use in populations so we don't need redis, mgrep, 4store, etc
class RI::Population::LabelConverter
  alias_method :old_convert, :convert
  def convert(*args)
    result = old_convert(*args)
    $test_converted[args.first] = result
    result
  end
end

module RI::Population::Elasticsearch
  alias_method :old_store_documents, :store_documents
  def store_documents
    @es_queue.each do |doc|
      doc[:annotations][:direct].each do |direct|
        $test_annotation_counts[direct] ||= 0
        $test_annotation_counts[direct] += 1
      end
      doc[:annotations][:ancestors].each do |anc|
        $test_annotation_counts_anc[anc] ||= 0
        $test_annotation_counts_anc[anc] += 1
      end
    end
    old_store_documents
  end
end

class RI::Population::Mgrep::Client
  alias_method :old_annotate, :annotate
  def annotate(*args)
    result = old_annotate(*args)
    $test_annotations[args.first] = result
    result
  end
end

class RI::Population::Class
  alias_method :old_retrieve_ancestors, :retrieve_ancestors
  def retrieve_ancestors(*args)
    result = old_retrieve_ancestors(*args)
    $test_ancestors[self.xxhash] = result
    result
  end
end

class RI::Population::Manager
  alias_method :old_latest_submissions_sparql, :latest_submissions_sparql
  def latest_submissions_sparql(*args)
    result = old_latest_submissions_sparql(*args)
    $test_latest_sub = result
    result
  end
end

class RI::GenerateTestData < RI::TestCase
  def test_data
    require 'logger'
    @res = RI::Resource.find("AE_test")
    populator = RI::Population::Manager.new(@res,
    {
      annotator_redis_host: "ncbostage-redis1",
      mgrep_host: "ncbostage-mgrep3",
      goo_host: "ncbostage-4store1",
      goo_port: 8080,
      population_threads: 1,
      bulk_index_size: 500,
      log_level: Logger::DEBUG
    })
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete
  end

  Minitest.after_run do
    binding.pry
    File.open(Dir.pwd + "/converted.dump", 'w') {|f| f.write(Marshal.dump($test_converted)) }
    File.open(Dir.pwd + "/annotations.dump", 'w') {|f| f.write(Marshal.dump($test_annotations)) }
    File.open(Dir.pwd + "/ancestors.dump", 'w') {|f| f.write(Marshal.dump($test_ancestors)) }
    File.open(Dir.pwd + "/annotation_counts.dump", 'w') {|f| f.write(Marshal.dump($test_annotation_counts)) }
    File.open(Dir.pwd + "/annotation_counts_anc.dump", 'w') {|f| f.write(Marshal.dump($test_annotation_counts_anc)) }
    File.open(Dir.pwd + "/latest_sub.dump", 'w') {|f| f.write(Marshal.dump($test_latest_sub)) }
  end
end