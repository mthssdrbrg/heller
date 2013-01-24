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

		# correlation_id, client_id, replica_id, max_wait, min_bytes
		def fetch_request(correlation_id, client_id, replica_id, max_wait, min_bytes, request_info)
			Kafka::Api::FetchRequest.new(topic, partition, offset, max_size)
		end

		def consume(topic, partition, offset, max_size = MAX_FETCH_SIZE)
			request = fetch_request(topic, partition, offset, max_size)

			messages = fetch(request)
			messages.to_a
		end

		def multi_fetch(topics_hash, max_size = MAX_FETCH_SIZE)
			requests = topics_hash.map { |topic, hash| fetch_request(topic, hash[:partition], hash[:offset], max_size) }

			response = multifetch(ArrayList.new(requests))
			parse_multi_fetch_response(topics_hash.keys, response)
		end

		protected

		def single_offset(topic, partition, time)
			offsets = get_offsets_before(topic, partition, time, 1)
			offsets = offsets.to_a

			offsets.first unless offsets.empty?
		end

		def parse_multi_fetch_response(topics, response)
			if response.respond_to?(:to_a)
				response_array = topics.zip(response.to_a)

				response_array.inject({}) do |response_hash, (topic, messages)| 
					response_hash[topic] = messages.to_a
					response_hash
				end
			else
				puts 'response does not respond to #to_a'
				puts "response is #{response.inspect}"
				puts "topics were: #{topics.inspect}"
			end
		end

		private

		def partition_fetch_info(offset, fetch_size = MAX_FETCH_SIZE)
			PartitionFetchInfo.new(offset, fetch_size)
		end

		def topic_and_partition(topic, partition = 0)
			TopicAndPartition.new(topic, partition)
		end
	end
end
