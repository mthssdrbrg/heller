# encoding: utf-8

require 'spec_helper'


module Heller
  describe ZookeeperConsumer do
    let :consumer do
      described_class.new(options, consumer_impl)
    end

    let :options do
      {
        zk_connect: 'localhost:2181',
        group_id: 'test',
      }
    end

    let :consumer_impl do
      double(:consumer_impl)
    end

    let :values do
      []
    end

    before do
      allow(consumer_impl).to receive(:createJavaConsumerConnector).and_return(consumer_impl)
      allow(consumer_impl).to receive(:create_message_streams) do |hash, *args|
        values.concat(hash.values)
      end
      allow(consumer_impl).to receive(:create_message_streams_by_filter)
    end

    describe '#initialize' do
      it 'creates a JavaConsumerConnector' do
        described_class.new(options, consumer_impl)

        expect(consumer_impl).to have_received(:createJavaConsumerConnector)
      end
    end

    describe '#create_streams' do
      context 'when given :key_decoder and :value_decoder' do
        let :key_decoder do
          double(:key_decoder)
        end

        let :value_decoder do
          double(:value_decoder)
        end

        before do
          consumer.create_streams({}, key_decoder: key_decoder, value_decoder: value_decoder)
        end

        it 'creates message streams with given key decoder' do
          expect(consumer_impl).to have_received(:create_message_streams).with({}, key_decoder, anything)
        end

        it 'creates message streams with given value decoder' do
          expect(consumer_impl).to have_received(:create_message_streams).with({}, anything, value_decoder)
        end

        it 'converts longs to integers' do
          values.each do |value|
            expect(value).to be_a(java.lang.Integer)
          end
        end
      end

      context 'when not given any options' do
        before do
          consumer.create_streams({'topic1' => 2})
        end

        it 'creates message streams' do
          expect(consumer_impl).to have_received(:create_message_streams)
        end

        it 'converts longs to integers' do
          values.each do |value|
            expect(value).to be_a(java.lang.Integer)
          end
        end
      end
    end

    describe '#create_streams_by_filter' do
      let :key_decoder do
        nil
      end

      let :value_decoder do
        nil
      end

      before do
        consumer.create_streams_by_filter('hello-world', 1, key_decoder: key_decoder, value_decoder: value_decoder)
      end

      it 'creates message streams' do
        expect(consumer_impl).to have_received(:create_message_streams_by_filter)
      end

      it 'creates a Whitelist object from given filter' do
        expect(consumer_impl).to have_received(:create_message_streams_by_filter).with(instance_of(Kafka::Consumer::Whitelist), 1)
      end

      context 'when given :key_decoder and :value_decoder' do
        let :key_decoder do
          double(:key_decoder)
        end

        let :value_decoder do
          double(:value_decoder)
        end

        it 'creates message streams with given key decoder' do
          expect(consumer_impl).to have_received(:create_message_streams_by_filter).with(anything, 1, key_decoder, anything)
        end

        it 'creates message streams with given value decoder' do
          expect(consumer_impl).to have_received(:create_message_streams_by_filter).with(anything, 1, anything, value_decoder)
        end
      end
    end

    describe '#commit' do
      before do
        allow(consumer_impl).to receive(:commit_offsets)
      end

      it 'calls #commit_offsets' do
        consumer.commit

        expect(consumer_impl).to have_received(:commit_offsets)
      end
    end

    describe '#close' do
      before do
        allow(consumer_impl).to receive(:shutdown)
      end

      it 'calls #shutdown' do
        consumer.close

        expect(consumer_impl).to have_received(:shutdown)
      end

      it 'is aliased as #shutdown' do
        consumer.shutdown

        expect(consumer_impl).to have_received(:shutdown)
      end
    end
  end
end
