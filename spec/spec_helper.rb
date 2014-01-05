# encoding: utf-8

require 'json'
require 'spec/support/fakers'

RSpec.configure do |config|
  config.include(Fakers)
  config.order = 'random'
end

require 'coveralls'
require 'simplecov'

if ENV.include?('TRAVIS')
  Coveralls.wear!
  SimpleCov.formatter = Coveralls::SimpleCov::Formatter
end

SimpleCov.start do
  add_group 'Source', 'lib'
  add_group 'Unit tests', 'spec/heller'
  add_group 'Integration tests', 'spec/integration'
end

require 'heller'
