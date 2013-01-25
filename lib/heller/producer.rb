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
			self.send(wrap_message(topic, key, message))
		end

		def multiple(*messages)
			wrapped_messages = messages.map do |topic, message, key|
				wrap_message(topic, key, message)
			end

			self.send(wrapped_messages)
		end

		def multiple_to(topic, messages)
			wrapped_messages = messages.map do |message|
				wrap_message(topic, nil, message)
			end

			self.send(wrapped_messages)
		end

		protected

		def wrap_message(topic, key, message)
			Kafka::Producer::KeyedMessage.new(topic, key, message)
		end

		def hash_to_properties(options)
			properties = java.util.Properties.new

			options.each do |key, value|
				properties.put(key.to_s, value.to_s)
			end

			properties
		end
	end
end
