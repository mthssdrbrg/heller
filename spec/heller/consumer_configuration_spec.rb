# encoding: utf-8

require 'spec_helper'


module Heller
  describe ConsumerConfiguration do
    let :configuration do
      described_class.new(options).to_java
    end

    describe '#to_java' do
      shared_examples_for 'a ConsumerConfiguration' do
        it 'returns a Kafka::Consumer::ConsumerConfig object' do
          expect(configuration).to be_a(Kafka::Consumer::ConsumerConfig)
        end

        it 'sets #auto_commit_enable' do
          expect(configuration.auto_commit_enable).to be false
        end

        it 'sets #auto_commit_interval_ms' do
          expect(configuration.auto_commit_interval_ms).to eq(1000)
        end

        it 'sets #auto_offset_reset' do
          expect(configuration.auto_offset_reset).to eq('smallest')
        end

        it 'sets #client_id' do
          expect(configuration.client_id).to eq('spec-client-id')
        end

        it 'sets #consumer_timeout_ms' do
          expect(configuration.consumer_timeout_ms).to eq(30)
        end

        it 'sets #fetch_min_bytes' do
          expect(configuration.fetch_min_bytes).to eq(12345)
        end

        it 'sets #fetch_message_max_bytes' do
          expect(configuration.fetch_message_max_bytes).to eq(2048)
        end

        it 'sets #fetch_wait_max_ms' do
          expect(configuration.fetch_wait_max_ms).to eq(54321)
        end

        it 'sets #group_id' do
          expect(configuration.group_id).to eq('spec-group')
        end

        it 'sets #num_consumer_fetchers' do
          expect(configuration.num_consumer_fetchers).to eq(10)
        end

        it 'sets #queued_max_messages' do
          expect(configuration.queued_max_messages).to eq(1500)
        end

        it 'sets #rebalance_backoff_ms' do
          expect(configuration.rebalance_backoff_ms).to eq(125)
        end

        it 'sets #rebalance_max_retries' do
          expect(configuration.rebalance_max_retries).to eq(15)
        end

        it 'sets #socket_receive_buffer_bytes' do
          expect(configuration.socket_receive_buffer_bytes).to eq(4096)
        end

        it 'sets #socket_timeout_ms' do
          expect(configuration.socket_timeout_ms).to eq(100)
        end

        it 'sets #zk_connect' do
          expect(configuration.zk_connect).to eq('localhost:2181')
        end

        it 'sets #zk_session_timeout_ms' do
          expect(configuration.zk_session_timeout_ms).to eq(125)
        end

        it 'sets #zk_connection_timeout_ms' do
          expect(configuration.zk_connection_timeout_ms).to eq(150)
        end

        it 'sets #zk_sync_time_ms' do
          expect(configuration.zk_sync_time_ms).to eq(100)
        end

        it 'sets #partition_assignment_strategy' do
          expect(configuration.partition_assignment_strategy).to eq('range')
        end

        it 'sets #offsets_storage' do
          expect(configuration.offsets_storage).to eq('kafka')
        end

        it 'sets #offsets_channel_backoff_ms' do
          expect(configuration.offsets_channel_backoff_ms).to eq(1000)
        end

        it 'sets #offsets_channel_socket_timeout_ms' do
          expect(configuration.offsets_channel_socket_timeout_ms).to eq(1500)
        end

        it 'sets #offsets_commit_max_retries' do
          expect(configuration.offsets_commit_max_retries).to eq(15)
        end

        it 'sets #dual_commit_enabled' do
          expect(configuration.dual_commit_enabled).to be true
        end

        it 'sets #exclude_internal_topics' do
          expect(configuration.exclude_internal_topics).to be false
        end
      end

      context 'with Symbol keys' do
        let :options do
          {
            auto_commit: false,
            auto_commit_interval: 1000,
            auto_reset_offset: :smallest,
            client_id: 'spec-client-id',
            fetch_message_max_bytes: 2048,
            fetch_min_bytes: 12345,
            fetch_max_wait: 54321,
            group_id: 'spec-group',
            num_fetchers: 10,
            max_queued_message_chunks: 1500,
            rebalance_retries: 15,
            rebalance_retry_backoff: 125,
            receive_buffer: 4096,
            refresh_leader_backoff: 250,
            socket_timeout: 100,
            timeout: 30,
            zk_connect: 'localhost:2181',
            zk_session_timeout: 125,
            zk_connection_timeout: 150,
            zk_sync_time: 100,
            partition_assignment_strategy: 'range',
            offsets_storage: 'kafka',
            offsets_channel_backoff_ms: 1000,
            offsets_channel_socket_timeout_ms: 1500,
            offsets_commit_max_retries: 15,
            dual_commit_enabled: true,
            exclude_internal_topics: false,
          }
        end

        it_behaves_like 'a ConsumerConfiguration'
      end

      context 'when given String keys' do
        let :options do
          {
            'auto_commit' => false,
            'auto_commit_interval' => 1000,
            'auto_reset_offset' => :smallest,
            'client_id' => 'spec-client-id',
            'fetch_message_max_bytes' => 2048,
            'fetch_min_bytes' => 12345,
            'fetch_max_wait' => 54321,
            'group_id' => 'spec-group',
            'num_fetchers' => 10,
            'max_queued_message_chunks' => 1500,
            'rebalance_retries' => 15,
            'rebalance_retry_backoff' => 125,
            'receive_buffer' => 4096,
            'refresh_leader_backoff' => 250,
            'socket_timeout' => 100,
            'timeout' => 30,
            'zk_connect' => 'localhost:2181',
            'zk_session_timeout' => 125,
            'zk_connection_timeout' => 150,
            'zk_sync_time' => 100,
            'partition_assignment_strategy' => 'range',
            'offsets_storage' => 'kafka',
            'offsets_channel_backoff_ms' => 1000,
            'offsets_channel_socket_timeout_ms' => 1500,
            'offsets_commit_max_retries' => 15,
            'dual_commit_enabled' => true,
            'exclude_internal_topics' => false,
          }
        end

        it_behaves_like 'a ConsumerConfiguration'
      end
    end
  end
end
