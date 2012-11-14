# encoding: utf-8

require 'java'
require 'kafka-jars'

module Kafka
	module Api
		java_import 'kafka.api.FetchRequest'
		java_import 'kafka.api.MultiFetchRequest'
		java_import 'kafka.api.MultiFetchResponse'
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

		java_import 'kafka.javaapi.producer.ProducerData'

		java_import 'kafka.javaapi.producer.SyncProducer'
		java_import 'kafka.javaapi.producer.Producer'
	end
end
