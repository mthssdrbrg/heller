require 'spec_helper'

module Heller

	describe Producer do

		let(:producer) { Producer.new('localhost:9092', 'serializer.class' => 'kafka.serializer.StringEncoder') }
		let(:consumer) { Consumer.new('localhost', 2181, 1000, 1024, 'heller-test-client') }
		let(:random_suffix) { Random.rand(2**16) }

		context 'when producing data', :pending => 'TopicConsumer' do

			it 'produces data' do
				producer.single("heller-test-topic-#{random_suffix}", 'Heller test data')
			end
		end
		
	end
end
