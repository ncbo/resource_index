require 'pry'
require 'minitest/autorun'
require_relative '../lib/ncbo_resource_index'

# Kept for test data consistency
Annotator = RI::Population

require_relative 'shared_data'
require_relative 'population_setup'

# Prevent Persisted::Hash from writing data while testing
Persisted::Hash.prevent_persist

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
  class TestCase < Minitest::Test
    TOTAL_ES_RECORDS = 962104

    def setup
      Dir.glob(Dir.pwd + "/ae_test*resume").each {|f| File.delete(f)}
      RI::Population::Document.fail_on_index(false)
      RI.config(sqlite: true)
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
      RI.db.run(DOCUMENTS_TEST_DATA.force_encoding('UTF-8'))
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
      RI::Population::Document.fail_on_index(false)
      RI.db[:obr_resource].delete
      RI.db[:obr_ae_test_element].delete
      RI.db[:obs_ontology].delete
      RI.db[:obs_concept].delete
      if @es && @index_id
        @es.indices.delete index: @index_id
      end
      db_file = Dir.pwd+"/ri_test.db"
      File.delete(db_file) if File.exist?(db_file)
    end
  end
end

