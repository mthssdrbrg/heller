# encoding: utf-8

require 'spec_helper'

module Heller
  describe ProducerConfiguration do
    it 'has sane defaults for running locally' do
      configuration = described_class.new

      configuration[:brokers].should == 'localhost:9092'
      configuration[:serializer].should == 'kafka.serializer.StringEncoder'
      configuration[:ack].should == -1
      configuration[:type].should == :sync
    end

    context 'given hash with user-defined options' do
      it 'merges them with the defaults' do
        configuration = described_class.new({
          brokers: 'localhost:9092,localhost:9093',
          serializer: 'kafka.serializer.DefaultEncoder',
          batch_size: 1500
        })

        configuration[:brokers].should == 'localhost:9092,localhost:9093'
        configuration[:serializer].should == 'kafka.serializer.DefaultEncoder'
        configuration[:batch_size].should == 1500
      end
    end

    context '#to_java' do
      let :configuration do
        described_class.new({
          brokers: 'localhost:9092,localhost:9093',
          type: :async,
          serializer: 'kafka.serializer.StringEncoder',
          key_serializer: 'kafka.serializer.DefaultEncoder',
          partitioner: 'kafka.producer.DefaultPartitioner',
          compression: :gzip,
          num_retries: 5,
          retry_backoff: 1500,
          metadata_refresh_interval: 5000,
          batch_size: 2000,
          client_id: 'spec-client',
          request_timeout: 10000,
          buffer_limit: 100 * 100,
          buffer_timeout: 1000 * 100,
          enqueue_timeout: 1000,
          socket_buffer: 1024 * 1000,
          ack: -1
        })
      end

      it 'returns an instance of Kafka::Producer::ProducerConfig' do
        configuration.to_java.should be_a(Kafka::Producer::ProducerConfig)
      end

      it 'converts Ruby options to their corresponding Kafka specific option' do
        producer_config = configuration.to_java
        producer_config.broker_list.should == 'localhost:9092,localhost:9093'
        producer_config.request_required_acks.should == -1
        producer_config.producer_type.should == 'async'
        producer_config.serializer_class.should == 'kafka.serializer.StringEncoder'
        producer_config.key_serializer_class.should == 'kafka.serializer.DefaultEncoder'
        producer_config.partitioner_class.should == 'kafka.producer.DefaultPartitioner'
        producer_config.compression_codec.name.should == 'gzip'
        producer_config.message_send_max_retries.should == 5
        producer_config.retry_backoff_ms.should == 1500
        producer_config.topic_metadata_refresh_interval_ms.should == 5000
        producer_config.queue_buffering_max_ms.should == 1000 * 100
        producer_config.queue_buffering_max_messages.should == 10000
        producer_config.queue_enqueue_timeout_ms.should == 1000
        producer_config.batch_num_messages.should == 2000
        producer_config.send_buffer_bytes.should == 1024 * 1000
        producer_config.client_id.should == 'spec-client'
        producer_config.request_timeout_ms.should == 10000
      end
    end
  end
end
