module Heller
	class Producer < Kafka::Producer::Producer

		attr_reader :configuration

		def initialize(zk_connect, options = {})
			options.merge!({
				'zk.connect' => zk_connect
			})

			@configuration = Kafka::Producer::ProducerConfig.new(hash_to_properties(options))
			super @configuration
		end

		def produce(topic_mappings)
			producer_data = topic_mappings.map do |topic, messages|
				Kafka::Producer::ProducerData.new(topic, messages)
			end

			send(producer_data)
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
