require 'pry'
require 'minitest/autorun'
require_relative '../lib/resource_index'
Persisted::Hash.prevent_persist

module RI
  class TestCase < Minitest::Test

  end
end

