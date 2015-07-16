# encoding: utf-8

require 'slf4j-jars'
require 'kafka-jars'


module Kafka
  module Api
    java_import 'kafka.api.FetchRequest'
    java_import 'kafka.api.FetchRequestBuilder'
    java_import 'kafka.api.PartitionOffsetRequestInfo'
    java_import 'kafka.api.OffsetRequest'
  end

  module JavaApi
    java_import 'kafka.javaapi.FetchResponse'
    java_import 'kafka.javaapi.OffsetRequest'
    java_import 'kafka.javaapi.OffsetResponse'
    java_import 'kafka.javaapi.PartitionMetadata'
    java_import 'kafka.javaapi.TopicMetadata'
    java_import 'kafka.javaapi.TopicMetadataRequest'
    java_import 'kafka.javaapi.TopicMetadataResponse'
  end

  module Common
    java_import 'kafka.common.TopicAndPartition'
  end

  module Serializer
    java_import 'kafka.serializer.StringEncoder'
    java_import 'kafka.serializer.StringDecoder'
  end

  module Consumer
    include_package 'kafka.consumer'
    java_import 'kafka.javaapi.consumer.SimpleConsumer'
  end

  module Message
    java_import 'kafka.message.Message'
    java_import 'kafka.javaapi.message.MessageSet'
    java_import 'kafka.javaapi.message.ByteBufferMessageSet'
  end

  module Metrics
    java_import 'kafka.metrics.KafkaMetricsGroup'

    module KafkaMetricsGroup
      def self.remove_all_consumer_metrics(client_id)
        self.removeAllConsumerMetrics(client_id)
      end
    end
  end

  module Producer
    java_import 'kafka.javaapi.producer.Producer'
    java_import 'kafka.producer.ProducerConfig'
    java_import 'kafka.producer.SyncProducerConfig'
    java_import 'kafka.producer.KeyedMessage'
  end

  module Errors
    java_import 'kafka.common.ErrorMapping'
  end
end