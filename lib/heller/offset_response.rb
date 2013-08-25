# encoding: utf-8

module Heller
  class OffsetResponse
    def initialize(underlying)
      @underlying = underlying
    end

    def offsets(topic, partition)
      convert_error { @underlying.offsets(topic, partition).to_a }
    end

    def error?
      @underlying.has_error?
    end

    def error(topic, partition)
      convert_error { @underlying.error_code(topic, partition) }
    end

    private

    def convert_error
      yield
    rescue NoSuchElementException => e
      raise NoSuchTopicPartitionCombinationError, e.message, e.backtrace
    end
  end
end
