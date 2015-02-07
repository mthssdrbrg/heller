# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'heller/version'

Gem::Specification.new do |s|
  s.name        = 'heller'
  s.version     = Heller::VERSION
  s.authors     = ['Mathias Söderberg']
  s.email       = ['mths@sdrbrg.se']
  s.homepage    = 'https://github.com/mthssdrbrg/heller'
  s.summary     = %q{JRuby wrapper for Kafka}
  s.description = %q{Attempts to make Kafka's Java API a bit more Rubyesque}
  s.license     = 'Apache License 2.0'

  s.files         = Dir['lib/**/*.rb', 'README.md']
  s.test_files    = Dir['spec/**/*.rb']
  s.require_paths = %w(lib)

  s.platform    = 'java'

  s.add_runtime_dependency 'kafka-jars'
end
