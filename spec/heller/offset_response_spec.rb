# encoding: utf-8

require 'spec_helper'

module Heller
  describe OffsetResponse do
    let :response do
      described_class.new(underlying)
    end

    let :underlying do
      double(:offset_response)
    end

    before do
      underlying.stub(:has_error?)
    end

    describe '#error?' do
      it 'asks proxies the underlying FetchResponse#has_error?' do
        response.error?

        expect(underlying).to have_received(:has_error?)
      end
    end

    describe '#error' do
      context 'given a topic and partition combination that does exist' do
        it 'returns whatever the underlying FetchResponse returns' do
          expect(underlying).to receive(:error_code).with('spec', 0).and_return('error stuff')

          expect(response.error('spec', 0)).to eq('error stuff')
        end
      end

      context 'given a topic and partition combination that does not exist' do
        it 'raises NoSuchTopicPartitionCombinationError' do
          expect(underlying).to receive(:error_code).with('non-existent', 1024).and_raise(NoSuchElementException.new)

          expect { response.error('non-existent', 1024) }.to raise_error(NoSuchTopicPartitionCombinationError)
        end
      end
    end

    describe '#offsets' do
      context 'given a topic and partition combination that does exist' do
        let :fake_long_array do
          double(:long_array, to_a: [])
        end

        it 'returns an array' do
          expect(underlying).to receive(:offsets).with('spec', 0).and_return(fake_long_array)

          expect(response.offsets('spec', 0)).to eq([])
          expect(fake_long_array).to have_received(:to_a)
        end
      end

      context 'given a topic and partition combination that does not exist' do
        it 'raises NoSuchTopicPartitionCombinationError' do
          expect(underlying).to receive(:offsets).with('non-existent', 1024).and_raise(NoSuchElementException.new)

          expect { response.offsets('non-existent', 1024) }.to raise_error(NoSuchTopicPartitionCombinationError)
        end
      end
    end
  end
end
