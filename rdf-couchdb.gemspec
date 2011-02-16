#!/usr/bin/env ruby -rubygems
# -*- encoding: utf-8 -*-

GEMSPEC = Gem::Specification.new do |gem|
  gem.version            = '0.0.5'

  gem.name               = 'rdf-couchdb'
  gem.homepage           = 'https://github.com/ipublic/rdf-couchdb'
  gem.description        = 'RDF.rb plugin providing a CouchDB storage adapter.'
  gem.summary            = 'RDF.rb plugin providing a CouchDB storage adapter.'

  gem.authors            = ['Greg Lappen', 'Dan Thomas']
  gem.email              = ['greg@lapcominc.com', 'dan.thomas@ipublic.org']

  gem.platform           = Gem::Platform::RUBY
  gem.files              = %w(README.md) + Dir.glob('lib/**/*.rb')
  gem.bindir             = %q(bin)
  gem.executables        = %w()
  gem.default_executable = gem.executables.first
  gem.require_paths      = %w(lib)
  gem.extensions         = %w()
  gem.test_files         = %w()
  gem.has_rdoc           = false

  gem.required_ruby_version      = '>= 1.8.7'
  gem.requirements               = []
  gem.add_runtime_dependency     'rdf',          '>= 0.3.1'
  gem.add_runtime_dependency     'couchrest',    '>= 1.0.1'
  gem.add_development_dependency 'rdf-spec',     '>= 0.3.1'
  gem.add_development_dependency 'rspec',        '>= 2.1.0'
  gem.post_install_message       = nil
end
