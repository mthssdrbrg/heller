# encoding: utf-8

require 'securerandom'

module Heller
  class Consumer
    def initialize(host, port, options = {})
      @host, @port = host, port
      @options  = defaults.merge(options)
      @consumer = create_consumer(@options)
      @builder  = create_builder(@options)
      @decoder  = Kafka::Serializer::StringDecoder.new(nil)
    end

    def client_id
      @consumer.client_id
    end

    def fetch(topic, partition, offset, fetch_size = DEFAULT_FETCH_SIZE)
      request     = @builder.add_fetch(topic, partition, offset, fetch_size).build
      response    = @consumer.fetch(request)
      message_set = response.message_set(topic, partition)

      MessageSetEnumerator.new(message_set, @decoder)
    end

    private

    DEFAULT_FETCH_SIZE = 1024 * 1024

    def defaults
      {
        timeout: 30 * 1000,
        buffer_size: 64 * 1024,
        client_id: generate_client_id
      }
    end

    def generate_client_id
      "heller-#{self.class.name.split('::').last.downcase}-#{SecureRandom.uuid}"
    end

    def create_consumer(options)
      consumer_impl = options.delete(:consumer_impl) || Kafka::Consumer::SimpleConsumer
      extra_options = options.values_at(:timeout, :buffer_size, :client_id)
      consumer_impl.new(@host, @port, *extra_options)
    end

    def create_builder(options)
      builder = Kafka::Api::FetchRequestBuilder.new

      [:client_id, :max_wait, :min_bytes].each do |symbol|
        builder.send(symbol, options[symbol]) if options[symbol]
      end

      builder
    end
  end
end
