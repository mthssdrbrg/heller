# encoding: utf-8

module Heller
  class Producer
    def initialize(broker_list, options = {})
      @producer = create_producer(options.merge(brokers: broker_list))
    end

    def push(messages)
      @producer.send(ArrayList.new(Array(messages)))
    end

    def disconnect
      @producer.close
    end
    alias_method :close, :disconnect

    private

    def create_producer(options)
      producer_impl = options.delete(:producer_impl) || Kafka::Producer::Producer
      producer_impl.new(ProducerConfiguration.new(options).to_java)
    end
  end
end