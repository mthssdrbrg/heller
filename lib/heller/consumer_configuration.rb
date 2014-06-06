# encoding: utf-8

module Heller
  class ConsumerConfiguration
    def initialize(configuration={})
      @configuration = configuration
    end

    def [](key)
      @configuration[key]
    end

    def to_java
      convert_to_properties
    end

    private

    KEY_MAPPINGS = {
      auto_commit: 'auto.commit.enable',
      auto_commit_interval: 'auto.commit.interval.ms',
      auto_reset_offset: 'auto.offset.reset',
      client_id: 'client.id',
      consumer_id: 'consumer.id',
      fetch_max_bytes: 'fetch.message.max.bytes',
      fetch_min_bytes: 'fetch.min.bytes',
      fetch_wait_max: 'fetch.wait.max.ms',
      group_id: 'group.id',
      num_fetchers: 'num.consumer.fetchers',
      num_retries: 'rebalance.max.retries',
      queue_max_messages: 'queued.max.message.chunks',
      receive_buffer: 'socket.receive.buffer.bytes',
      retry_backoff: 'rebalance.backoff.ms',
      refresh_leader_backoff: 'refresh.leader.backoff.ms',
      socket_timeout: 'socket.timeout.ms',
      timeout: 'consumer.timeout.ms',
      zk_connect: 'zookeeper.connect',
      zk_session_timeout: 'zookeeper.session.timeout.ms',
      zk_connection_timeout: 'zookeeper.connection.timeout.ms',
      zk_sync_time: 'zookeeper.sync.time.ms',
    }.freeze

    def convert_to_properties
      props = @configuration.each_with_object(Properties.new) do |(key, value), props|
        props.put(KEY_MAPPINGS[key], value.to_s)
      end

      Kafka::Consumer::ConsumerConfig.new(props)
    end
  end
end
