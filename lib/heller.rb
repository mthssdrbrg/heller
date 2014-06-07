# encoding: utf-8

require 'kafka'


module Heller
  java_import 'java.util.ArrayList'
  java_import 'java.util.Properties'
  java_import 'java.lang.IllegalArgumentException'
  java_import 'java.util.NoSuchElementException'

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
require 'heller/consumer_configuration'
require 'heller/producer'
require 'heller/producer_configuration'
require 'heller/errors'
require 'heller/fetch_request'
require 'heller/fetch_response'
require 'heller/topic_metadata_response'
require 'heller/message'
require 'heller/message_set_enumerator'
require 'heller/offset_request'
require 'heller/offset_response'
require 'heller/consumer'
require 'heller/zookeeper_consumer'
require 'heller/version'