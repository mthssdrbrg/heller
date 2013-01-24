require 'spec_helper'

module Heller

	describe Consumer do

		let(:consumer) { Consumer.new('localhost', 9092, 1000, 1024, 'client_id') }
		let(:fake_response) { mock(Kafka::Api::FetchResponse) }

		before do
			fake_response.stub(:message_set).and_return(Array.new)
			consumer.stub(:fetch).and_return(fake_response)
		end

		context 'offset methods', :pending => 'OffsetRequest' do

			let(:offsets) { [0, 1, 2, 3, 4, 5] }

			before do
				consumer.stub(:get_offsets_before).and_return(offsets)
			end

			describe '#earliest_offset' do

				it 'should return earliest offset available' do
					offset = consumer.earliest_offset('0', 0)

					offset.should eq(offsets.first)
				end
			end

			describe '#latest_offset' do

				it 'should return latest offset available' do
					offset = consumer.latest_offset('0', 0)

					offset.should eq(offsets.last)				
				end
			end
		end

		describe '#consume' do

			let(:topic) { '0' }
			let(:partition) { 0 }
			let(:offset) { 0 }

			it 'should return an empty array' do
				fetched = consumer.consume(topic, partition, offset)
				fetched.should be_empty
			end
		end

		describe '#multi_consume' do

			context 'given a hash of topic to partition and offset mappings' do

				let(:topics_hash) do 
					{
						'0' => {
							:partition => 0,
							:offset => 0
						},
						'1' => {
							:partition => 0,
							:offset => 0
						}
					}
				end

				before(:each) do
					fake_response.stub(:data).and_return do
						topics_hash.inject({}) do |memo, (topic, options)|
							topic_partition = Kafka::Common::TopicAndPartition.new(topic, options[:partition])

							memo[topic_partition] = []
							memo
						end
					end
				end

				it 'returns a topic <-> message_set hash' do
					response = consumer.multi_consume(topics_hash)

					response.length.should eq(topics_hash.length)

					response['0'].should have_key(0)
					response['0'][0].should be_empty

					response['1'].should have_key(0)
					response['1'][0].should be_empty
				end
			end
		end

	end
end
