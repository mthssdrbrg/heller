# encoding: utf-8

require 'spec_helper'

module Heller
  describe FetchRequest do
    describe '#initialize' do
      it 'takes a topic, partition and offset' do
        fetch_request = described_class.new('spec', 0, 0)

        expect(fetch_request.topic).to eq('spec')
        expect(fetch_request.partition).to eq(0)
        expect(fetch_request.offset).to eq(0)
      end
    end
  end
end
