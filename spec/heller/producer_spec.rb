# encoding: utf-8
require 'spec_helper'

module Heller
  describe Producer do
    let(:producer) do
      described_class.new('localhost:9092', producer_impl: producer_impl)
    end

    let :producer_impl do
      double(:producer_impl, new: producer_spy)
    end

    let :producer_spy do
      double(:producer, send: nil)
    end

    describe '#new' do
      let :producer_config do
        ProducerConfiguration.new(brokers: 'localhost:9092,localhost:9093', type: :async).to_java
      end

      it 'converts options to a Kafka::Producer::ProducerConfig object' do
        described_class.new('localhost:9092,localhost:9093', type: :async, producer_impl: producer_impl)

        expect(producer_impl).to have_received(:new) { |config| config.should == producer_config }
      end
    end

    describe '#push' do
      it 'wraps messages in a java.util.ArrayList' do
        messages = [Heller::Message.new('topic', 'actual message', 'key!'), Heller::Message.new('topic2', 'payload')]

        producer.push(messages)

        expect(producer_spy).to have_received(:send) do |msgs|
          msgs.should be_a(java.util.ArrayList)
          msgs.to_a.should == messages
        end
      end

      it 'allows sending a single message' do
        message = Heller::Message.new('topic', 'actual message')

        producer.push(message)

        expect(producer_spy).to have_received(:send) do |msg|
          msg.should have(1).item
          msg.first.should == message
        end
      end
    end
  end
end
