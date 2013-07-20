# encoding: utf-8

require 'securerandom'

module Heller
  class Consumer
    def initialize(host, port, options = {})
      @host, @port = host, port
      options   = defaults.merge(options)
      @consumer = create_consumer(options)
      @builder  = create_builder(options)
      @decoder  = Kafka::Serializer::StringDecoder.new(nil)
    end

    def client_id
      @consumer.client_id
    end

    def fetch(fetch_hash, fetch_size = DEFAULT_FETCH_SIZE)
      fetch_hash.each do |(topic, partition), offset|
        @builder.add_fetch(topic, partition, offset, fetch_size)
      end

      response = @consumer.fetch(@builder.build)

      fetch_hash.each_key.with_object({}) do |topic_partition, memo|
        message_set = response.message_set(*topic_partition)
        memo[topic_partition] = MessageSetEnumerator.new(message_set, @decoder)
      end
    end

    def metadata(topics)
      unless topics.empty?
        request = Kafka::Api::TopicMetadataRequest.new(topics)
        @consumer.send(request)
      end
    end

    def offsets_before(offsets_hash, max_offsets = 1)
      request_info = offsets_hash.each_with_object({}) do |(topic_partition, time), memo|
        topic_partition = Kafka::Common::TopicAndPartition.new(*topic_partition)
        partition_offset = Kafka::Api::PartitionOffsetRequestInfo.new(time.to_i, max_offsets)
        memo[topic_partition] = partition_offset
      end

      request = Kafka::Api::OffsetRequest.new(request_info, Heller::OffsetRequest.current_version, client_id)
      @consumer.get_offsets_before(request)
    end

    def earliest_offset(topics_partitions)
      offsets_before(create_offsets_hash(topics_partitions, Heller::OffsetRequest.earliest_time))
    end

    def latest_offset(topics_partitions)
      offsets_before(create_offsets_hash(topics_partitions, Heller::OffsetRequest.latest_time))
    end

    def disconnect
      @consumer.close
    end
    alias_method :close, :disconnect

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

    def create_offsets_hash(topics_partitions, magic_offset)
      topics_partitions.each_with_object({}) do |topic_partition, memo|
        memo[topic_partition] = magic_offset
      end
    end
  end
end
