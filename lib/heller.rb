# encoding: utf-8

require 'kafka'

module Heller
  java_import 'java.util.ArrayList'
  java_import 'java.util.Properties'

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

  java_import 'kafka.api.OffsetRequest'

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
end

require 'heller/producer'
require 'heller/producer_configuration'
require 'heller/message_set_enumerator'
require 'heller/consumer'