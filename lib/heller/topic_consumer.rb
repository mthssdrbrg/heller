module Heller
	class TopicConsumer < Consumer

		DEFAULT_PARTITION = 0.freeze

		def consume(request_hash)
			fetch_request = build_fetch_request(request_hash)
			fetch_response = self.fetch(fetch_request)

			extract_message_sets(fetch_response)
		end

		private

		def build_fetch_request(request_hash, options = {})
			builder = Kafka::Api::FetchRequestBuilder.new.client_id(self.client_id)

			request_hash.each do |topic, offset|
				builder.add_fetch(topic, options.fetch(:partition, DEFAULT_PARTITION), offset, options.fetch(:fetch_size, MAX_FETCH_SIZE))
			end

			builder.build
		end

		def extract_message_sets(fetch_response)
			extracted = {}
			keys_iterator = fetch_response.data.keys.iterator

			while keys_iterator.has_next?
				key = keys_iterator.next
				topic, partition = key.topic, key.partition

				extracted[topic] = fetch_response.message_set(topic, partition).iterator
			end

			extracted
		end
	end
end
