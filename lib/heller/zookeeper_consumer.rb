# encoding: utf-8

module Heller
  class ZookeeperConsumer
    def initialize(options, consumer_impl=Kafka::Consumer::Consumer)
      @consumer = create_consumer(consumer_impl, options)
    end

    def create_streams(topic_count_map, options={})
      if options[:key_decoder] && options[:value_decoder]
        @consumer.create_message_streams(topic_count_map, *options.values_at(:key_decoder, :value_decoder))
      else
        @consumer.create_message_streams(topic_count_map)
      end
    end

    def commit
      @consumer.commit_offsets
    end

    def close
      @consumer.shutdown
    end
    alias_method :shutdown, :close

    private

    def create_consumer(consumer_impl, options)
      consumer_impl.createJavaConsumerConnector(ConsumerConfiguration.new(options).to_java)
    end
  end
end
