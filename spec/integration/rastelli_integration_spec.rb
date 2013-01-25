require 'spec_helper'

module Heller

	describe 'producing and consuming data' do

		let(:producer) { Producer.new('localhost:9092', 'serializer.class' => 'kafka.serializer.StringEncoder') }
		let(:consumer) { Consumer.new('localhost', 9092, 1000, 1024, 'heller-test-client') }
		let(:decoder)  { Kafka::Serializer::StringDecoder.new(nil) }
		let(:random_suffix) { Random.rand(2**16) }
		let(:test_topic) { "heller-test-topic-#{random_suffix}"}

		context 'when producing' do
			context 'single message' do

				it 'produces data' do
					producer.single(test_topic, 'Heller test data')

					messages = consumer.consume(test_topic, 0, 0)
					messages.empty?.should be_false
					message_and_offset = messages.first

					message_and_offset.offset.should == 0
					message_and_offset.next_offset.should == 1

					payload = message_and_offset.message.payload
					payload_bytes = Java::byte[payload.remaining].new
					payload.get(payload_bytes)
					decoded_message = decoder.from_bytes(payload_bytes)
					decoded_message.should eq('Heller test data')
				end
			end

			context 'multiple messages to multiple topics and partitions', :pending => true do
			end

			describe '#multiple_to' do

				let :messages_to_produce do
					20.times.map { |i| "Producer test message #{i}" }
				end

				it 'produces several messages' do
					producer.multiple_to(test_topic, messages_to_produce)

					messages = consumer.consume(test_topic, 0, 0)
					messages.empty?.should be_false
					messages.length.should == 20

					20.times do |i|
						message_and_offset = messages[i]

						message_and_offset.offset.should eq(i)
						message_and_offset.next_offset.should eq(i + 1)

						payload = message_and_offset.message.payload
						payload_bytes = Java::byte[payload.remaining].new
						payload.get(payload_bytes)

						decoded_message = decoder.from_bytes(payload_bytes)
						decoded_message.should eq("Producer test message #{i}")
					end
				end
			end
		end

		context 'when consuming data' do

			describe '#consume' do

				before do
					producer.single(test_topic, 'Heller consumer test data')
				end

				it 'fetches data' do
					messages = consumer.consume(test_topic, 0, 0)
					messages.empty?.should be_false
					message_and_offset = messages.first

					message_and_offset.offset.should == 0
					message_and_offset.next_offset.should == 1

					payload = message_and_offset.message.payload
					payload_bytes = Java::byte[payload.remaining].new
					payload.get(payload_bytes)

					decoded_message = decoder.from_bytes(payload_bytes)
					decoded_message.should eq('Heller consumer test data')
				end
			end

			describe '#multi_consume' do

				let :produced_hash do
					3.times.inject({}) do |memo, i|
						memo["heller-test-topic-#{random_suffix + i}"] = 5.times.map { |j| "Consumer test message #{random_suffix + i}-#{j}" }
						memo
					end
				end

				let :request_hash do
					3.times.inject({}) do |memo, i|
						memo["heller-test-topic-#{random_suffix + i}"] = [{ :offset => 0	}]
						memo
					end
				end

				before do
					produced_hash.each do |topic, messages|
						producer.multiple_to(topic, messages)
					end
				end

				it 'fetches messages' do
					message_hash = consumer.multi_consume(request_hash)

					message_hash.length.should eq(request_hash.length)
					message_hash.each do |topic, partitions_and_messages|
						partitions_and_messages.values.each do |messages|
							messages.each_with_index do |message_and_offset, index|
								payload = message_and_offset.message.payload
								payload_bytes = Java::byte[payload.remaining].new
								payload.get(payload_bytes)

								decoded_message = decoder.from_bytes(payload_bytes)
								decoded_message.should eq("Consumer test message #{topic.split('-').last}-#{index}")
							end
						end
						
					end
				end
			end
		end

	end
end
