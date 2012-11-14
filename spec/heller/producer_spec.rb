require 'spec_helper'

module Heller

	describe Producer do

		describe '#new' do

			it 'should create a configuration from required arguments' do
				expect {
					producer = Producer.new('localhost:2181')

					producer.configuration.should be_instance_of(Kafka::Producer::ProducerConfig)
					producer.configuration.zk_connect.should eq('localhost:2181')
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

		describe '#produce' do

			let(:producer) { Producer.new('localhost:2181') }
			let(:topics_hash) do 
				{
					'0' => ['Test 1', 'Test 2', 'Test 3']
				}
			end

			it 'should send messages to given topic' do
				producer.should_receive(:send).with(kind_of(Array)).and_return(nil)

				expect {
					producer.produce(topics_hash)
				}.to_not raise_error
			end

		end
	end
end
