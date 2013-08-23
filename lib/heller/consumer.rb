# encoding: utf-8

require 'securerandom'

module Heller
  class Consumer
    def initialize(connect_string, options = {})
      @host, @port = connect_string.split(':')
      options   = defaults.merge(options)
      @consumer = create_consumer(options)
      @builder  = create_builder(options)
      @decoder  = Kafka::Serializer::StringDecoder.new(nil)
      @build_lock = Concurrency::Lock.new
    end

    def client_id
      @consumer.client_id
    end

    def fetch(fetch_requests, fetch_size = DEFAULT_FETCH_SIZE)
      kafka_fetch_request = @build_lock.lock do
        Array(fetch_requests).each do |request|
          @builder.add_fetch(request.topic, request.partition, request.offset, fetch_size)
        end

        @builder.build
      end

      FetchResponse.new(@consumer.fetch(kafka_fetch_request), @decoder)
    end

    def metadata(topics)
      unless topics.empty?
        request = Kafka::JavaApi::TopicMetadataRequest.new(topics)
        @consumer.send(request)
      end
    end

    def offsets_before(offset_requests)
      request_info = Array(offset_requests).each_with_object({}) do |request, memo|
        topic_partition = Kafka::Common::TopicAndPartition.new(request.topic, request.partition)
        partition_offset = Kafka::Api::PartitionOffsetRequestInfo.new(request.time.to_i, request.max_offsets)

        memo[topic_partition] = partition_offset
      end

      request = Kafka::JavaApi::OffsetRequest.new(request_info, Heller::OffsetRequest.current_version, client_id)
      @consumer.get_offsets_before(request)
    end

    def earliest_offset(topic, partition)
      offsets_before(Heller::OffsetRequest.new(topic, partition, Heller::OffsetRequest.earliest_time))
    end

    def latest_offset(topic, partition)
      offsets_before(Heller::OffsetRequest.new(topic, partition, Heller::OffsetRequest.latest_time))
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
      consumer_impl.new(@host, @port.to_i, *extra_options)
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
