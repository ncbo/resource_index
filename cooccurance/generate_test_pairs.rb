require 'pry'
require 'minitest/autorun'
require_relative '../lib/ncbo_resource_index'

# Kept for test data consistency
Annotator = RI::Population

# Less logging
$ri_log_level = Logger::FATAL

require_relative 'shared_data_cooccurance'
# require_relative '../test/population_setup'

module ResourceIndex
  def self.setup_sql_client
    if RUBY_PLATFORM == "java"
      @client = Sequel.connect("jdbc:sqlite:ri_test.db")
    else
      @client = Sequel.connect("sqlite:ri_test.db")
    end
  end
end

module RI
  class GenerateCooccurance < Minitest::Test
    def setup
      db_file = Dir.pwd+"/ri_test.db"
      File.delete(db_file) if File.exist?(db_file)
      @resource_store = "test_resource_store_#{Time.now.to_i}"
      RI.config(sqlite: true, resource_store: @resource_store)
      RI.db.create_table :obr_resource do
        primary_key :id
        String :name
        String :resource_id
        String :structure
        String :main_context
        String :url
        String :element_url
        String :description
        String :logo
        Integer :dictionary_id
        Integer :total_element
        Time :last_update_date
        Time :workflow_completed_date
      end
      RI.db.run(RESOURCES_TEST_DATA)
      RI.db.create_table :obr_ae_test_element do
        primary_key :id
        String :local_element_id
        Integer :dictionary_id
        String :ae_name
        String :ae_description
        String :ae_species
        String :ae_experiment_type
      end
      RI.db.run(DOCUMENTS_TEST_DATA_AE.force_encoding('UTF-8'))
      RI.db.create_table :obr_pm_element do
        Integer :id
        String :local_element_id
        String :pm_title
        String :pm_abstract
        String :pm_keywords
        String :pm_meshheadings
      end
      RI.db.run(DOCUMENTS_TEST_DATA_PM.force_encoding('UTF-8'))
      RI.db.create_table :obs_ontology do
        primary_key :id
        String :local_ontology_id
        String :name
        String :version
        String :description
        Integer :status
        String :virtual_ontology_id
        String :format
        Integer :dictionary_id
      end
      RI.db.run(ONTOLOGIES_TABLE)
      RI.db.create_table :obs_concept do
        primary_key :id
        String :local_concept_id
        Integer :ontology_id
        Integer :is_toplevel
        String :full_id
      end
      RI.db.run(CONCEPTS_TABLE)
    end

    def teardown
      RI.es.indices.delete(index: @resource_store)
      RI.es.indices.delete(index: @ae_index_id)
      db_file = Dir.pwd+"/ri_test.db"
      File.delete(db_file) if File.exist?(db_file)
    end

    def test_generate_pairs
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
        log_level: Logger::DEBUG,
        write_label_pairs: true,
        write_class_pairs: true
      })
      @ae_index_id = populator.populate()
      sleep(2) # wait for indexing to complete
    end

    # $label_counts = []
    # $cls_counts = []
    # $label_ind_counts = {}
    # Minitest.after_run do
    #   sorted = $label_counts.sort
    #   len = sorted.length
    #   puts "Label min / max / avg / median", $label_counts.min, $label_counts.max, ($label_counts.inject{ |sum, el| sum + el }.to_f / $label_counts.size), (sorted[(len - 1) / 2] + sorted[len / 2]) / 2.0
    #   sorted = $cls_counts.sort
    #   len = sorted.length
    #   puts "Cls min / max / avg / median", $cls_counts.min, $cls_counts.max, ($cls_counts.inject{ |sum, el| sum + el }.to_f / $cls_counts.size), (sorted[(len - 1) / 2] + sorted[len / 2]) / 2.0
    #   binding.pry
    # end

  end
end