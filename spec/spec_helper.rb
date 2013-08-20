# encoding: utf-8

require 'simplecov'
SimpleCov.start

require 'heller'
require 'spec/support/fakers'

RSpec.configure do |config|
  config.include(Fakers)
end
