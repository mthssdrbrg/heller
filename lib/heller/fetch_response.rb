# encoding: utf-8

module Heller
  class FetchResponse
    def initialize(underlying, decoder)
      @underlying, @decoder = underlying, decoder
    end

    def error?
      @underlying.has_error?
    end

    def error(topic, partition)
      @underlying.error_code(topic, partition)
    rescue IllegalArgumentException => e
      raise NoSuchTopicPartitionCombinationError, e.message, e.backtrace
    end

    def messages(topic, partition)
      MessageSetEnumerator.new(@underlying.message_set(topic, partition), @decoder)
    rescue IllegalArgumentException => e
      raise NoSuchTopicPartitionCombinationError, e.message, e.backtrace
    end

    def high_watermark(topic, partition)
      @underlying.high_watermark(topic, partition)
    rescue IllegalArgumentException => e
      raise NoSuchTopicPartitionCombinationError, e.message, e.backtrace
    end
  end
end
