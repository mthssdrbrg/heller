# encoding: utf-8

require 'bundler/setup'
require 'json'
require 'spec/support/fakers'

RSpec.configure do |config|
  config.include(Fakers)
  config.order = 'random'
  config.raise_errors_for_deprecations!
  config.before(:suite) do
    unless java.lang.System.getProperty('log4j.configuration')
      @log4j_config_file = Tempfile.new('silent-log4j.properties')
      @log4j_config_file.puts('log4j.rootLogger=OFF')
      @log4j_config_file.rewind
      java.lang.System.setProperty('log4j.configuration', %(file://#{@log4j_config_file.path}))
    end
  end
  config.after(:suite) do
    if @log4j_config_file
      @log4j_config_file.close
      @log4j_config_file.unlink
    end
  end
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
