class Consumer < Kafka::Consumer::SimpleConsumer
			
	LATEST_OFFSET = -1
	EARLIEST_OFFSET = -2

	MAX_FETCH_SIZE = 1000000.freeze

	def latest_offset(topic, partition)
		single_offset(topic, partition, LATEST_OFFSET)
	end

	def earliest_offset(topic, partition)
		single_offset(topic, partition, EARLIEST_OFFSET)
	end

	def fetch_request(topic, partition, offset, max_size = MAX_FETCH_SIZE)
		Kafka::Api::FetchRequest.new(topic, partition, offset, max_size)
	end

	def fetch(fetch_request)
		messages = super
		messages.to_a
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
