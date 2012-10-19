require 'spec_helper'

module Heller

	describe Consumer do

		let(:consumer) { Consumer.new('localhost', 9092, 10000, 1024000) }

		context 'added offset methods' do

			before do
				consumer.stub(:get_offsets_before).and_return(offsets)
			end

			describe '#earliest_offset' do

				let(:offsets) { [0, 1, 2, 3, 4, 5] }

				it 'should return earliest offset available' do
					offset = consumer.earliest_offset('0', 0)

					offset.should eq(offsets.first)
				end
			end

			describe '#latest_offset' do

				let(:offsets) { [5, 4, 3, 2, 1] }

				it 'should return latest offset available' do
					offset = consumer.latest_offset('0', 0)

					offset.should eq(offsets.first)				
				end
			end
		end

		describe '#fetch_request' do

			let(:topic) { '0' }
			let(:partition) { 0 }
			let(:offset) { 0 }
			let(:max_size) { 1024000 }

			it 'should create a Kafka::Api::FetchRequest' do
				fetch_request = consumer.fetch_request(topic, partition, offset, max_size)

				fetch_request.should be_kind_of(Kafka::Api::FetchRequest)
				fetch_request.topic.should eq(topic)
				fetch_request.partition.should eq(partition)
				fetch_request.offset.should eq(offset)
				fetch_request.max_size.should eq(max_size)
			end

			it 'should set default max fetch size if none given' do
				fetch_request = consumer.fetch_request(topic, partition, offset)

				fetch_request.max_size.should eq(Consumer::MAX_FETCH_SIZE)
			end
		end

		describe '#fetch' do

			let(:fetch_request) { consumer.fetch_request('0', 0, 0) }

			before do
				Kafka::Consumer::SimpleConsumer.any_instance.stub(:fetch).and_return(java.util.ArrayList.new)
			end

			it 'should return an array', :pending => true do
				fetched = consumer.fetch(fetch_request)

				fetched.should be_kind_of(Array)
				fetched.should be_empty
			end

		end

	end
end
