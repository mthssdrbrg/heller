# encoding: utf-8

module Heller
  class OffsetRequest
    attr_reader :topic, :partition, :time, :max_offsets

    def self.latest_time
      Kafka::Api::OffsetRequest.latest_time
    end

    def self.earliest_time
      Kafka::Api::OffsetRequest.earliest_time
    end

    def self.current_version
      Kafka::Api::OffsetRequest.current_version
    end

    def initialize(topic, partition, time, offsets = 1)
      @topic, @partition, @time, @max_offsets = topic, partition, time, offsets
    end
  end
end
