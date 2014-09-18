# encoding: utf-8

require 'securerandom'


module Heller
  class Consumer
    def initialize(connect_string, options = {})
      @host, @port = connect_string.split(':')
      options   = defaults.merge(options)
      @consumer = create_consumer(options)
      @build_options = options.select { |k, _| BUILD_OPTIONS.include?(k) }
      @decoder  = Kafka::Serializer::StringDecoder.new(nil)
    end

    def client_id
      @consumer.client_id
    end

    def fetch(fetch_requests, fetch_size = DEFAULT_FETCH_SIZE)
      builder = create_builder(@build_options)
      Array(fetch_requests).each do |request|
        builder.add_fetch(request.topic, request.partition, request.offset, fetch_size)
      end
      raw_response = @consumer.fetch(builder.build)
      FetchResponse.new(raw_response, @decoder)
    end

    def metadata(topics=[])
      request = Kafka::JavaApi::TopicMetadataRequest.new(topics)
      TopicMetadataResponse.new(@consumer.send(request))
    end
    alias_method :topic_metadata, :metadata

    def offsets_before(offset_requests)
      request_info = Array(offset_requests).each_with_object({}) do |request, memo|
        topic_partition = Kafka::Common::TopicAndPartition.new(request.topic, request.partition)
        partition_offset = Kafka::Api::PartitionOffsetRequestInfo.new(request.time.to_i, request.max_offsets)

        memo[topic_partition] = partition_offset
      end

      request = Kafka::JavaApi::OffsetRequest.new(request_info, OffsetRequest.current_version, client_id)
      OffsetResponse.new(@consumer.get_offsets_before(request))
    end

    def earliest_offset(topic, partition)
      response = offsets_before(OffsetRequest.new(topic, partition, OffsetRequest.earliest_time))
      response.offsets(topic, partition).first
    end

    def latest_offset(topic, partition)
      response = offsets_before(OffsetRequest.new(topic, partition, OffsetRequest.latest_time))
      response.offsets(topic, partition).last
    end

    def disconnect
      @consumer.close
    end
    alias_method :close, :disconnect

    private

    DEFAULT_FETCH_SIZE = 1024 * 1024
    BUILD_OPTIONS = [:client_id, :max_wait, :min_bytes].freeze

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

      BUILD_OPTIONS.each do |symbol|
        builder.send(symbol, options[symbol]) if options[symbol]
      end

      builder
    end
  end
end
