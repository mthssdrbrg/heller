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
        described_class.new('localhost:9092,localhost:9093', type: :async, producer_impl: producer_impl)

        expect(producer_impl).to have_received(:new).with(instance_of(Kafka::Producer::ProducerConfig))
      end
    end

    describe '#push' do
      it 'wraps messages in a java.util.ArrayList' do
        messages = [Heller::Message.new('topic', 'actual message', 'key!'), Heller::Message.new('topic2', 'payload')]

        expect(producer_spy).to receive(:send) do |msgs|
          expect(msgs).to be_a(java.util.ArrayList)
          expect(msgs.to_a).to eq(messages)
        end

        producer.push(messages)
      end

      it 'allows sending a single message' do
        message = Heller::Message.new('topic', 'actual message')

        expect(producer_spy).to receive(:send) do |msgs|
          expect(msgs.size).to eq(1)
          expect(msgs.first).to eq(message)
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
