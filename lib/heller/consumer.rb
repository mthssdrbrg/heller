module Heller
	class Consumer < Kafka::Consumer::SimpleConsumer
				
		LATEST_OFFSET = -1.freeze
		EARLIEST_OFFSET = -2.freeze

		MAX_FETCH_SIZE = 1000000.freeze

		def initialize(*args)
			super(*args)

			@builder = Kafka::Api::FetchRequestBuilder.new
		end

		def latest_offset(topic, partition)
			single_offset(topic, partition, LATEST_OFFSET)
		end

		def earliest_offset(topic, partition)
			single_offset(topic, partition, EARLIEST_OFFSET)
		end

		def consume(topic, partition, offset, fetch_size = MAX_FETCH_SIZE)
			@builder.add_fetch(topic, partition, offset, fetch_size)

			fetch_request = @builder.build
			fetch_response = self.fetch(fetch_request)

			fetch_response.message_set(topic, partition).to_a
		end

		def multi_consume(topics_hash)
			topics_hash.each do |topic, options|
				@builder.add_fetch(topic, options.fetch(:partition, 0), options[:offset], options.fetch(:fetch_size, MAX_FETCH_SIZE))
			end

			fetch_request = @builder.build
			fetch_response = self.fetch(fetch_request)

			extract_message_sets(fetch_response)
		end

		protected

		def single_offset(topic, partition, time)
			offsets = get_offsets_before(topic, partition, time, 1)
			offsets = offsets.to_a

			offsets.first unless offsets.empty?
		end

		private

		def extract_message_sets(fetch_response)
			extracted = Hash.new { |hash, key| hash[key] = Hash.new }
			
			fetch_response.data.each do |topic_partition, data|
				topic = topic_partition.topic
				partition = topic_partition.partition

				extracted[topic][partition] = fetch_response.message_set(topic, partition)
			end

			extracted
		end
	end
end
