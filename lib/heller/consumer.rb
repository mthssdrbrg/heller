module Heller
	class Consumer < Kafka::Consumer::SimpleConsumer
				
		LATEST_OFFSET = -1.freeze
		EARLIEST_OFFSET = -2.freeze

		MAX_FETCH_SIZE = 1000000.freeze

		def consume(topic, partition, offset, fetch_size = MAX_FETCH_SIZE)
			request_hash = {
				topic => [{
					:partition => partition,
					:offset => offset,
					:fetch_size => fetch_size
				}]
			}

			fetch_request = build_fetch_request(request_hash)
			fetch_response = self.fetch(fetch_request)

			fetch_response.message_set(topic, partition).iterator.to_a
		end

		def multi_consume(request_hash)
			fetch_request = build_fetch_request(request_hash)
			fetch_response = self.fetch(fetch_request)

			extract_message_sets(fetch_response)
		end

		protected

		def build_fetch_request(request_hash)
			builder = Kafka::Api::FetchRequestBuilder.new.client_id(self.client_id)

			request_hash.each do |topic, partitions|
				partitions.each do |partition_opts|
					builder.add_fetch(topic, partition_opts.fetch(:partition, 0), partition_opts[:offset], partition_opts.fetch(:fetch_size, MAX_FETCH_SIZE))
				end
			end

			builder.build
		end

		def extract_message_sets(fetch_response)
			extracted = Hash.new { |hash, key| hash[key] = Hash.new }
			keys_iterator = fetch_response.data.keys.iterator

			while keys_iterator.has_next?
				key = keys_iterator.next
				topic, partition = key.topic, key.partition

				extracted[topic][partition] = fetch_response.message_set(topic, partition).iterator
			end

			extracted
		end
	end
end
