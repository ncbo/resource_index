require 'pry'
require 'minitest/autorun'
require_relative '../lib/resource_index'

# Kept for test data consistency
Annotator = RI::Population

require_relative 'shared_data'
require_relative 'population_setup'

# Prevent Persisted::Hash from writing data while testing
Persisted::Hash.prevent_persist

module RI
  class TestCase < Minitest::Test
    TOTAL_ES_RECORDS = 962104

    def setup
      Dir.glob(Dir.pwd + "/ae_test*resume").each {|f| File.delete(f)}
      RI::Document.fail_on_index(false)
      ResourceIndex.config(adapter: "amalgalite", username: "test", password: "test", database: "")
      ResourceIndex.db.create_table :obr_resource do
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
      ResourceIndex.db.run(RESOURCES_TEST_DATA)
      ResourceIndex.db.create_table :obr_ae_test_element do
        primary_key :id
        String :local_element_id
        Integer :dictionary_id
        String :ae_name
        String :ae_description
        String :ae_species
        String :ae_experiment_type
      end
      ResourceIndex.db.run(DOCUMENTS_TEST_DATA.force_encoding('UTF-8'))
      ResourceIndex.db.create_table :obs_ontology do
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
      ResourceIndex.db.run(ONTOLOGIES_TABLE)
      ResourceIndex.db.create_table :obs_concept do
        primary_key :id
        String :local_concept_id
        Integer :ontology_id
        Integer :is_toplevel
        String :full_id
      end
      ResourceIndex.db.run(CONCEPTS_TABLE)
    end

    def teardown
      RI::Document.fail_on_index(false)
      ResourceIndex.db[:obr_resource].delete
      ResourceIndex.db[:obr_ae_test_element].delete
      ResourceIndex.db[:obs_ontology].delete
      ResourceIndex.db[:obs_concept].delete
      if @es && @index_id
        @es.indices.delete index: @index_id
      end
    end
  end
end

