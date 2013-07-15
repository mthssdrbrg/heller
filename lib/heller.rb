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
end

require 'heller/producer'
require 'heller/producer_configuration'
require 'heller/message_set_enumerator'
require 'heller/consumer'