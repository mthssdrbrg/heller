# encoding: utf-8

module Heller
  class Message < Kafka::Producer::KeyedMessage
    def initialize(topic, message, key = nil)
      super(topic, key, message)
    end
  end
end
