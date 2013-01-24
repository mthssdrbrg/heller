# encoding: utf-8

require 'java'
require 'kafka-jars'

module Kafka
	module Api
		java_import 'kafka.javaapi.FetchRequest'
		java_import 'kafka.javaapi.FetchResponse'
	end

	module Common
		java_import 'kafka.common.TopicAndPartition'
	end

	module Consumer
		java_import 'kafka.javaapi.consumer.SimpleConsumer'
	end

	module Message
		java_import 'kafka.message.Message'
		java_import 'kafka.javaapi.message.MessageSet'
		java_import 'kafka.javaapi.message.ByteBufferMessageSet'
	end

	module Producer
		java_import 'kafka.producer.ProducerConfig'
		java_import 'kafka.producer.SyncProducerConfig'
		java_import 'kafka.producer.KeyedMessage'
		java_import 'kafka.javaapi.producer.Producer'
	end
end
