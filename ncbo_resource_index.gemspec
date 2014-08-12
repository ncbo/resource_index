# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'ncbo_resource_index/version'

Gem::Specification.new do |spec|
  spec.name          = "ncbo_resource_index"
  spec.version       = ResourceIndex::VERSION
  spec.authors       = ["Paul R Alexander"]
  spec.email         = ["palexander@stanford.edu"]
  spec.description   = %q{NCBO Resource Index}
  spec.summary       = %q{Library for working with NCBO's Resource Index backend system}
  spec.homepage      = "http://github.com/ncbo/resource_index"
  spec.license       = "BSD 2-clause"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "sequel"
  spec.add_dependency "ruby-xxHash"
  spec.add_dependency "elasticsearch"
  spec.add_dependency "typhoeus"
  spec.add_dependency "pony"
  spec.add_dependency "ref"

  spec.add_dependency "mysql2" if RUBY_PLATFORM != "java"
  spec.add_dependency "sqlite3" if RUBY_PLATFORM != "java"

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "minitest"
  spec.add_development_dependency "pry"
end
