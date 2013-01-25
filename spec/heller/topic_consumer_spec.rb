require 'spec_helper'

module Heller

	describe TopicConsumer do

		class FakeIterator

			def initialize(topics)
				@topics_and_partitions = topics.inject([]) do |memo, topic|
					memo << Kafka::Common::TopicAndPartition.new(topic, 0)
					memo
				end
			end

			def has_next?
				@topics_and_partitions.any?
			end

			def next
				@topics_and_partitions.shift
			end
		end

		let(:consumer) { TopicConsumer.new('localhost', 9092, 1000, 1024, 'topic-client-id') }
		
		let(:fake_response) do 
			mock(Kafka::Api::FetchResponse).tap do |f|
				f.stub_chain(:data, :keys, :iterator).and_return(FakeIterator.new(topics_and_offsets.keys))
				f.stub(:message_set).and_return(fake_message_set)
			end
		end

		let(:fake_message_set) do
			mock(Kafka::Message::ByteBufferMessageSet).tap do |b|
				b.stub(:iterator).and_return([])
			end
		end

		describe '#consume' do

			before do
				consumer.stub(:fetch).and_return(fake_response)
			end

			let(:topics_and_offsets) do
				{
					'test-topic0' => 0,
					'test-topic1' => 1,
					'test-topic2' => 2
				}
			end

			it 'consumes messages from given topics-and-offsets hash' do
				fetched = consumer.consume(topics_and_offsets)

				fetched.length.should eq(topics_and_offsets.length)
				fetched.keys.should eq(topics_and_offsets.keys)
				fetched.values.should eq([[], [], []])
			end
		end

	end
end
