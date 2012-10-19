module Heller
	class Consumer < Kafka::Consumer::SimpleConsumer
				
		LATEST_OFFSET = -1.freeze
		EARLIEST_OFFSET = -2.freeze

		MAX_FETCH_SIZE = 1000000.freeze

		def latest_offset(topic, partition)
			single_offset(topic, partition, LATEST_OFFSET)
		end

		def earliest_offset(topic, partition)
			single_offset(topic, partition, EARLIEST_OFFSET)
		end

		def fetch_request(topic, partition, offset, max_size)
			Kafka::Api::FetchRequest.new(topic, partition, offset, max_size)
		end

		def consume(topic, partition, offset, max_size = MAX_FETCH_SIZE)
			request = fetch_request(topic, partition, offset, max_size)

			messages = fetch(request)
			messages.to_a
		end

		def multi_fetch(topics_hash, max_size = MAX_FETCH_SIZE)
			requests = topics_hash.map { |topic, hash| fetch_request(topic, hash[:partition], hash[:offset], max_size) }

			response = multifetch(java.util.ArrayList.new(requests))
			parse_multi_fetch_response(topics_hash.keys, response)
		end

		protected

		def single_offset(topic, partition, time)
			offsets = get_offsets_before(topic, partition, time, 1)
			offsets = offsets.to_a

			offsets.first unless offsets.empty?
		end

		def parse_multi_fetch_response(topics, response)
			response_array = topics.zip(response.to_a)

			response_array.inject({}) do |response_hash, (topic, messages)| 
				response_hash[topic] = messages 
				response_hash
			end
		end
	end
end
