require 'pry'
require 'minitest/autorun'
require_relative '../lib/resource_index'
require_relative 'shared_data'

Persisted::Hash.prevent_persist

module RI
  class TestCase < Minitest::Test

  end
end

