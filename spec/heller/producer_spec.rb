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
      double(:producer, send: nil, close: nil)
    end

    describe '#new' do
      it 'converts options to a Kafka::Producer::ProducerConfig object' do
        expect(producer_impl).to receive(:new) { |config| config.should be_a(Kafka::Producer::ProducerConfig) }

        described_class.new('localhost:9092,localhost:9093', type: :async, producer_impl: producer_impl)
      end
    end

    describe '#push' do
      it 'wraps messages in a java.util.ArrayList' do
        messages = [Heller::Message.new('topic', 'actual message', 'key!'), Heller::Message.new('topic2', 'payload')]

        expect(producer_spy).to receive(:send) do |msgs|
          msgs.should be_a(java.util.ArrayList)
          msgs.to_a.should == messages
        end

        producer.push(messages)
      end

      it 'allows sending a single message' do
        message = Heller::Message.new('topic', 'actual message')

        expect(producer_spy).to receive(:send) do |msg|
          msg.should have(1).item
          msg.first.should == message
        end

        producer.push(message)
      end
    end

    describe '#disconnect' do
      it 'calls #close on the underlying producer' do
        producer.disconnect

        expect(producer_spy).to have_received(:close)
      end

      it 'is aliased to #close' do
        producer.close

        expect(producer_spy).to have_received(:close)
      end
    end
  end
end
