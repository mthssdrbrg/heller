# encoding: utf-8

module Heller
  class ProducerConfiguration
    def initialize(configuration = {})
      @configuration = defaults.merge(configuration.dup)
    end

    def [](key)
      @configuration[key]
    end

    def to_java
      convert_to_properties
    end

    private

    KEY_MAPPINGS = {
      brokers: 'metadata.broker.list',
      type: 'producer.type',
      serializer: 'serializer.class',
      key_serializer: 'key.serializer.class',
      partitioner: 'partitioner.class',
      compression: 'compression.codec',
      compressed_topics: 'compressed.topics',
      num_retries: 'message.send.max.retries',
      retry_backoff: 'retry.backoff.ms',
      metadata_refresh_interval: 'topic.metadata.refresh.interval.ms',
      batch_size: 'batch.num.messages',
      client_id: 'client.id',
      request_timeout: 'request.timeout.ms',
      buffer_limit: 'queue.buffering.max.messages',
      buffer_timeout: 'queue.buffering.max.ms',
      enqueue_timeout: 'queue.enqueue.timeout.ms',
      socket_buffer: 'send.buffer.bytes',
      ack: 'request.required.acks',
    }.freeze

    def defaults
      {
        brokers: 'localhost:9092',
        type: :sync,
        serializer: 'kafka.serializer.StringEncoder',
        ack: -1
      }
    end

    def convert_to_properties
      props = @configuration.each_with_object(Properties.new) do |(key, value), props|
        props.put(KEY_MAPPINGS[key], value.to_s)
      end

      Kafka::Producer::ProducerConfig.new(props)
    end
  end
end
