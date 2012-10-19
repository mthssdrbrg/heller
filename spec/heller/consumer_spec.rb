require 'spec_helper'

module Heller

	describe Consumer do

		let(:consumer) { Consumer.new('localhost', 9092, 10000, 1024000) }

		context 'offset methods' do

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
		end

		describe '#consume' do

			let(:topic) { '0' }
			let(:partition) { 0 }
			let(:offset) { 0 }

			it 'should return an array' do
				consumer.should_receive(:fetch).with(kind_of(Kafka::Api::FetchRequest)).and_return(java.util.ArrayList.new)

				fetched = consumer.consume(topic, partition, offset)

				fetched.should be_kind_of(Array)
				fetched.should be_empty
			end

		end

		describe '#multi_fetch' do

			let(:topics_hash) do 
				{
					'0' => {
						:partition => 0,
						:offset => 0
					},
					'1' => {
						:partition => 0,
						:offset => 0
					},
					'2' => {
						:partition => 0,
						:offset => 0
					}
				}
			end

			let(:multi_fetch_response) { mock('MultiFetchResponse') }
			let(:response) do

			end
			let(:expected_hash) do
				{
					'0' => [],
					'1' => [],
					'2' => []
				}
			end

			before do
				multi_fetch_response.stub(:to_a).and_return([[], [], []])
			end

			it 'should return a hash of topics and messages' do
				consumer.should_receive(:multifetch).with(kind_of(java.util.ArrayList)).and_return(multi_fetch_response)

				fetch_hash = consumer.multi_fetch(topics_hash)

				fetch_hash.should be_kind_of(Hash)
				fetch_hash.keys.should eq(topics_hash.keys)

				fetch_hash.values.each do |value|
					value.should be_kind_of(Array)
				end

				fetch_hash.should eq(expected_hash)
			end
		end
	end
end
