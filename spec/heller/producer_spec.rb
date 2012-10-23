require 'spec_helper'

module Heller

	describe Producer do

		describe '#new' do

			it 'should create a configuration from required arguments' do
				expect {
					producer = Producer.new('localhost', 9092)	

					producer.configuration.should be_instance_of(Kafka::Producer::SyncProducerConfig)
					producer.configuration.host.should eq('localhost')
					producer.configuration.port.should eq(9092)
				}.to_not raise_error
			end

			it 'should include optional options if any given' do
				expect {
					producer = Producer.new('localhost', 9092, 'serializer.class' => 'kafka.serializer.StringEncoder')

					producer.configuration.should be_instance_of(Kafka::Producer::SyncProducerConfig)
					producer.configuration.props.get('serializer.class').should eq('kafka.serializer.StringEncoder')
				}.to_not raise_error
			end
		end

		describe '#wrap_messages' do

			let(:producer) { Producer.new('localhost', 9092) }
			let(:messages) { ['Test 1', 'Test 2', 'Test 3'].map { |m| m.to_java_bytes } }

			it 'should wrap an array of messages in a ByteBufferMessageSet' do
				wrapped = producer.wrap_messages(messages)

				wrapped.should be_instance_of(Kafka::Message::ByteBufferMessageSet)
				wrapped.underlying.size.should eq(messages.size)
			end
		end

		describe '#produce' do

			let(:producer) { Producer.new('localhost', 9092) }
			let(:messages) { ['Test 1', 'Test 2', 'Test 3'].map { |m| m.to_java_bytes } }
			let(:topic) { '0' }
			let(:partition) { 0 }

			it 'should send messages to given topic' do
				producer.should_receive(:send).with(topic, kind_of(Kafka::Message::ByteBufferMessageSet)).and_return(nil)

				expect {
					producer.produce(topic, messages)
				}.to_not raise_error
			end

			it 'should send messages to given topic and partition' do
				producer.should_receive(:send).with(topic, partition, kind_of(Kafka::Message::ByteBufferMessageSet)).and_return(nil)

				expect {
					producer.produce(topic, messages, partition)
				}.to_not raise_error
			end

		end
	end
end
