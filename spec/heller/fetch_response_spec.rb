# encoding: utf-8

require 'spec_helper'

module Heller
  describe FetchResponse do
    let :fetch_response do
      described_class.new(underlying, decoder)
    end

    let :underlying do
      double(:fetch_response)
    end

    let :decoder do
      double(:decoder)
    end

    let :message_set do
      double(:message_set, iterator: nil)
    end

    before do
      underlying.stub(:has_error?)
      underlying.stub(:error)
      underlying.stub(:high_watermark)
    end

    describe '#error?' do
      it 'asks proxies the underlying FetchResponse#has_error?' do
        fetch_response.error?

        expect(underlying).to have_received(:has_error?)
      end
    end

    describe '#error' do
      context 'given a topic and partition combination that does exist' do
        it 'returns whatever the underlying FetchResponse returns' do
          expect(underlying).to receive(:error_code).with('spec', 0).and_return('error stuff')

          expect(fetch_response.error('spec', 0)).to eq('error stuff')
        end
      end

      context 'given a topic and partition combination that does not exist' do
        it 'raises NoSuchTopicPartitionCombinationError' do
          expect(underlying).to receive(:error_code).with('non-existent', 1024).and_raise(IllegalArgumentException.new)

          expect { fetch_response.error('non-existent', 1024) }.to raise_error(NoSuchTopicPartitionCombinationError)
        end
      end
    end

    describe '#messages' do
      context 'given a topic and partition combination that does exist' do
        it 'returns a MessageSetEnumerator' do
          expect(underlying).to receive(:message_set).with('spec', 0).and_return(message_set)

          enumerator = fetch_response.messages('spec', 0)

          expect(enumerator).to be_a(MessageSetEnumerator)
        end
      end

      context 'given a topic and partition combination that does not exist' do
        it 'raises NoSuchTopicPartitionCombinationError' do
          expect(underlying).to receive(:message_set).with('non-existent', 1024).and_raise(IllegalArgumentException.new)

          expect { fetch_response.messages('non-existent', 1024) }.to raise_error(NoSuchTopicPartitionCombinationError)
        end
      end
    end

    describe '#high_watermark' do
      context 'given a topic and partition combination that does exist' do
        it 'proxies the method call to the underlying FetchResponse' do
          fetch_response.high_watermark('spec', 0)

          expect(underlying).to have_received(:high_watermark).with('spec', 0)
        end
      end

      context 'given a topic and partition combination that does not exist' do
        it 'raises NoSuchTopicPartitionCombinationError' do
          expect(underlying).to receive(:high_watermark).with('non-existent', 1024).and_raise(IllegalArgumentException.new)

          expect { fetch_response.high_watermark('non-existent', 1024) }.to raise_error(NoSuchTopicPartitionCombinationError)
        end
      end
    end
  end
end
