# encoding: utf-8

module Heller
  class Configuration
    def initialize(options={})
      @configuration = defaults.merge(options)
    end

    def [](key)
      @configuration[key]
    end

    def to_java
      kafka_config_class.new(to_properties)
    end

    protected

    def defaults
      {}
    end

    private

    def to_properties
      @configuration.each_with_object(Properties.new) do |(key, value), props|
        props.put(key_mappings[key], value.to_s)
      end
    end
  end
end
