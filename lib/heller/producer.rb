module Heller
	class Producer < Kafka::Producer::Producer

		attr_reader :configuration

		def initialize(broker_list, options = {})
			options = ({
				'broker.list' => broker_list,
				'producer.type' => 'sync'
			}).merge(options)

			@configuration = Kafka::Producer::ProducerConfig.new(hash_to_properties(options))
			super(@configuration)
		end

		def single(topic, message, key = nil)
			send(Kafka::Producer::KeyedMessage.new(topic, key, message))
		end

		def multiple(*messages)
			wrapped_messages = messages.map do |topic, message, key|
				Kafka::Producer::KeyedMessage.new(topic, key, message)
			end

			send(wrapped_messages)
		end

		def multiple_to(topic, messages)
			wrapped_messages = messages.map do |message|
				Kafka::Producer::KeyedMessage.new(topic, nil, message)
			end

			send(wrapped_messages)
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
