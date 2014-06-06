# encoding: utf-8

require 'kafka'

module Heller
  java_import 'java.util.ArrayList'
  java_import 'java.util.Properties'
  java_import 'java.lang.IllegalArgumentException'
  java_import 'java.util.NoSuchElementException'

  class Message < Kafka::Producer::KeyedMessage
    def initialize(topic, message, key = nil)
      super(topic, key, message)
    end
  end

  class FetchRequest
    attr_reader :topic, :partition, :offset

    def initialize(*args)
      @topic, @partition, @offset = *args
    end
  end

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

  Errors = Kafka::Errors::ErrorMapping

  class Errors
    def self.error_for(code)
      self.exception_for(code)
    end
  end

  module Concurrency
    java_import 'java.util.concurrent.locks.ReentrantLock'

    class Lock
      def initialize
        @lock = Concurrency::ReentrantLock.new
      end

      def lock
        @lock.lock
        yield
      ensure
        @lock.unlock
      end
    end
  end

  HellerError = Class.new(StandardError)
  NoSuchTopicPartitionCombinationError = Class.new(HellerError)
end

require 'heller/configuration'
require 'heller/producer'
require 'heller/producer_configuration'
require 'heller/message_set_enumerator'
require 'heller/fetch_response'
require 'heller/topic_metadata_response'
require 'heller/offset_response'
require 'heller/consumer'
require 'heller/version'
require 'heller/consumer_configuration'