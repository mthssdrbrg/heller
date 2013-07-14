# encoding: utf-8

module Heller
  class Producer
    def initialize(broker_list, options = {})
      @options = options.merge(brokers: broker_list)
      @producer = create_producer(@options)
    end

    def push(messages)
      @producer.send(ArrayList.new(Array(messages)))
    end

    private

    def create_producer(options)
      producer_impl = options.delete(:producer_impl) || Kafka::Producer::Producer
      producer_impl.new(ProducerConfiguration.new(options).to_java)
    end
  end
end