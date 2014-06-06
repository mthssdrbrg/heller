# encoding: utf-8

require 'spec_helper'

module Heller
  describe ProducerConfiguration do
    it 'has sane defaults for running locally' do
      configuration = described_class.new

      expect(configuration[:brokers]).to eq('localhost:9092')
      expect(configuration[:serializer]).to eq('kafka.serializer.StringEncoder')
      expect(configuration[:ack]).to eq(-1)
      expect(configuration[:type]).to eq(:sync)
    end

    context 'given hash with user-defined options' do
      it 'merges them with the defaults' do
        configuration = described_class.new({
          brokers: 'localhost:9092,localhost:9093',
          serializer: 'kafka.serializer.DefaultEncoder',
          batch_size: 1500
        })

        expect(configuration[:brokers]).to eq('localhost:9092,localhost:9093')
        expect(configuration[:serializer]).to eq('kafka.serializer.DefaultEncoder')
        expect(configuration[:batch_size]).to eq(1500)
      end
    end

    context '#to_java' do
      let :configuration do
        described_class.new(options).to_java
      end

      let :options do
        {
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
        }
      end

      it 'returns an instance of Kafka::Producer::ProducerConfig' do
        expect(configuration).to be_a(Kafka::Producer::ProducerConfig)
      end

      it 'sets #broker_list' do
        expect(configuration.broker_list).to eq('localhost:9092,localhost:9093')
      end

      it 'sets #request_required_acks' do
        expect(configuration.request_required_acks).to eq(-1)
      end

      it 'sets #producer_type' do
        expect(configuration.producer_type).to eq('async')
      end

      it 'sets serializer_class' do
        expect(configuration.serializer_class).to eq('kafka.serializer.StringEncoder')
      end

      it 'sets #key_serializer_class' do
        expect(configuration.key_serializer_class).to eq('kafka.serializer.DefaultEncoder')
      end

      it 'sets #partitioner_class' do
        expect(configuration.partitioner_class).to eq('kafka.producer.DefaultPartitioner')
      end

      it 'sets #compression_codec' do
        expect(configuration.compression_codec.name).to eq('gzip')
      end

      it 'sets #message_send_max_retries' do
        expect(configuration.message_send_max_retries).to eq(5)
      end

      it 'sets #retry_backoff_ms' do
        expect(configuration.retry_backoff_ms).to eq(1500)
      end

      it 'sets #topic_metadata_refresh_interval_ms' do
        expect(configuration.topic_metadata_refresh_interval_ms).to eq(5000)
      end

      it 'sets #queue_buffering_max_ms' do
        expect(configuration.queue_buffering_max_ms).to eq(1000 * 100)
      end

      it 'sets #queue_buffering_max_messages' do
        expect(configuration.queue_buffering_max_messages).to eq(10000)
      end

      it 'sets #queue_enqueue_timeout_ms' do
        expect(configuration.queue_enqueue_timeout_ms).to eq(1000)
      end

      it 'sets #batch_num_messages' do
        expect(configuration.batch_num_messages).to eq(2000)
      end

      it 'sets #send_buffer_bytes' do
        expect(configuration.send_buffer_bytes).to eq(1024 * 1000)
      end

      it 'sets #client_id' do
        expect(configuration.client_id).to eq('spec-client')
      end

      it 'sets #request_timeout_ms' do
        expect(configuration.request_timeout_ms).to eq(10000)
      end
    end
  end
end
