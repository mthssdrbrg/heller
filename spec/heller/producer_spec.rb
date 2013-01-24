require 'spec_helper'

module Heller

	describe Producer do

		let(:producer) { Producer.new('localhost:2181') }

		describe '#new' do

			it 'should create a configuration from required arguments' do
				expect {
					producer = Producer.new('localhost:2181')

					producer.configuration.should be_instance_of(Kafka::Producer::ProducerConfig)
					producer.configuration.broker_list.should eq('localhost:2181')
				}.to_not raise_error
			end

			it 'should include optional options if any given' do
				expect {
					producer = Producer.new('localhost:2181', 'serializer.class' => 'kafka.serializer.StringEncoder')

					producer.configuration.should be_instance_of(Kafka::Producer::ProducerConfig)
					producer.configuration.serializer_class.should eq('kafka.serializer.StringEncoder')
				}.to_not raise_error
			end
		end

		describe '#single' do

			it 'sends message' do
				producer.should_receive(:send) do |keyed_message|
					keyed_message.should be_instance_of(Kafka::Producer::KeyedMessage)
					keyed_message.topic.should eq('1')
					keyed_message.message.should eq('Test message')
					keyed_message.key.should be_nil
				end

				producer.single('1', 'Test message')
			end
		end

		describe '#multiple' do

			it 'batches messages' do
				producer.should_receive(:send) do |keyed_messages|
					keyed_messages.each { |k| k.should be_instance_of(Kafka::Producer::KeyedMessage) }

					first_message = keyed_messages.first
					last_message = keyed_messages.last

					first_message.topic.should eq('1')
					first_message.message.should eq('Test message')
					first_message.key.should be_nil

					last_message.topic.should eq('2')
					last_message.message.should eq('Second message')
					last_message.key.should eq(1)
				end

				producer.multiple(['1', 'Test message'], ['2', 'Second message', 1])
			end			
		end

		describe '#multiple_to' do

			it 'batches messages going to same topic' do
				producer.should_receive(:send) do |keyed_messages|
					keyed_messages.each do |k| 
						k.should be_instance_of(Kafka::Producer::KeyedMessage) 
						k.key.should be_nil
						k.topic.should eq('test-topic')
						k.message.should be_kind_of(String)
					end
				end

				producer.multiple_to('test-topic', ['Test message', 'Second message'])				
			end
		end
	end
end
