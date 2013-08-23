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
      convert_error { @underlying.error_code(topic, partition) }
    end

    def messages(topic, partition)
      convert_error { MessageSetEnumerator.new(@underlying.message_set(topic, partition), @decoder) }
    end

    def high_watermark(topic, partition)
      convert_error { @underlying.high_watermark(topic, partition) }
    end

    private

    def convert_error
      yield
    rescue IllegalArgumentException => e
      raise NoSuchTopicPartitionCombinationError, e.message, e.backtrace
    end
  end
end
