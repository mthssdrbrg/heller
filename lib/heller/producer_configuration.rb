# encoding: utf-8

module Heller
  class ProducerConfiguration < Configuration

    protected

    def key_mappings
      @key_mappings ||= {
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
    end

    def defaults
      {
        brokers: 'localhost:9092',
        type: :sync,
        serializer: 'kafka.serializer.StringEncoder',
        ack: -1
      }
    end

    def kafka_config_class
      Kafka::Producer::ProducerConfig
    end
  end
end
