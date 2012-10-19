# encoding: utf-8

require 'java'
require 'kafka-jars'

module Kafka
	module Api
		java_import 'kafka.api.FetchRequest'
		java_import 'kafka.api.MultiFetchRequest'
		java_import 'kafka.api.MultiFetchResponse'
		java_import 'kafka.api.MultiProducerRequest'
		java_import 'kafka.api.OffsetRequest'
		java_import 'kafka.api.ProducerRequest'
	end

	module Consumer
		java_import 'kafka.javaapi.consumer.SimpleConsumer'

		class SimpleConsumer
			
			LATEST_OFFSET = -1
			EARLIEST_OFFSET = -2

			def latest_offset(topic, partition)
				single_offset(topic, partition, LATEST_OFFSET)
			end

			def earliest_offset(topic, partition)
				single_offset(topic, partition, EARLIEST_OFFSET)
			end

			private

			def single_offset(topic, partition, time)
				offsets = get_offsets_before(topic, partition, time, 1)
				offsets = offsets.to_a

				unless offsets.empty?
					offsets.first
				end
			end
		end
	end

	module Message
		java_import 'kafka.message.Message'
		java_import 'kafka.javaapi.message.MessageSet'
		java_import 'kafka.javaapi.message.ByteBufferMessageSet'
	end

	module Producer
		java_import 'kafka.producer.SyncProducerConfig'
		java_import 'kafka.javaapi.producer.SyncProducer'
	end
end
