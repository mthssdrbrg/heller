# encoding: utf-8

module Heller
  class Configuration
    def initialize(options={})
      @configuration = merge_with_defaults(options)
    end

    def [](key)
      @configuration[key.to_sym]
    end

    def to_java
      kafka_config_class.new(to_properties)
    end

    protected

    def defaults
      {}
    end

    private

    def merge_with_defaults(options)
      options.each_with_object(defaults) do |(k, v), h|
        h[k.to_sym] = v
      end
    end

    def to_properties
      @configuration.each_with_object(Properties.new) do |(key, value), props|
        props.put(key_mappings[key.to_sym], value.to_s)
      end
    end
  end
end
