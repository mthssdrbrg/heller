# encoding: utf-8

require 'spec_helper'

module Heller
  describe MessageSetEnumerator do
    let :message_set do
      create_fake_message_set('first', 'second', 'third', 'fourth')
    end

    let :decoder do
      Kafka::Serializer::StringDecoder.new(nil)
    end

    let :enumerator do
      MessageSetEnumerator.new(message_set, decoder)
    end

    describe '#next' do
      it 'returns the first offset and decoded message pair' do
        offset, message = enumerator.next
        expect(offset).to eq(0)
        expect(message).to eq('first')
      end

      it 'returns the second offset and decoded message pair' do
        enumerator.next
        offset, message = enumerator.next
        expect(offset).to eq(1)
        expect(message).to eq('second')
      end

      it 'returns each offset and decoded message pair in order' do
        result = []
        4.times { result << enumerator.next }
        expect(result).to eq([[0, 'first'], [1, 'second'], [2, 'third'], [3, 'fourth']])
      end

      it 'raises StopIteration when all pairs have been returned' do
        4.times { enumerator.next }
        expect { enumerator.next }.to raise_error(StopIteration)
        expect { enumerator.next }.to raise_error(StopIteration)
      end
    end

    describe '#each' do
      it 'returns each offset and decoded message pair' do
        result = []
        enumerator.each { |i| result << i }
        expect(result).to eq([[0, 'first'], [1, 'second'], [2, 'third'], [3, 'fourth']])
      end
    end
  end
end
