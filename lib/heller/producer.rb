module Heller
	class Producer < Kafka::Producer::SyncProducer

		attr_reader :configuration

		def initialize(host, port, options = {})
			options.merge!({
				'host' => host,
				'port' => port.to_s	
			})

			@configuration = Kafka::Producer::SyncProducerConfig.new(hash_to_properties(options))

			super(@configuration)
		end

		def wrap_messages(messages)
			converted = messages.map do |m| 
				Kafka::Message::Message.new(m) 
			end

			Kafka::Message::ByteBufferMessageSet.new(ArrayList.new(converted))
		end

		def produce(topic, messages, partition = :random)
			message_set = wrap_messages(messages)

			case partition
			when :random
				send(topic, message_set)
			when Integer
				send(topic, partition, message_set)
			end
		end

		protected

		def hash_to_properties(options)
			properties = java.util.Properties.new

			options.each do |key, value|
				properties.put(key.to_s, value.to_s)
			end

			properties
		end
	end
end
