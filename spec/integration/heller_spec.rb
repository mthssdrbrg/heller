# encoding: utf-8

require 'spec_helper'

require 'json'

module Heller
  describe 'end-to-end communication' do
    let :producer do
      Producer.new('localhost:9092', client_id: 'spec-producer')
    end

    let :consumer do
      Consumer.new('localhost:9092', client_id: 'spec-consumer')
    end

    context 'simple string messages' do
      let :topic do
        "spec-simple-string-#{Time.now.to_i.to_s(36)}"
      end

      it 'handles simple string messages' do
        producer.push(Heller::Message.new(topic, 'simple string message'))

        fetch_response = consumer.fetch(FetchRequest.new(topic, 0, 0))
        enumerator = fetch_response.messages(topic, 0)
        enumerator.should be_a(MessageSetEnumerator)

        messages = enumerator.to_a
        message = messages.last.last
        message.should eq('simple string message')
      end

      it 'handles JSON hashes' do
        producer.push(Heller::Message.new(topic, {'a key' => 'a value'}.to_json))

        fetch_response = consumer.fetch(FetchRequest.new(topic, 0, 0))
        enumerator = fetch_response.messages(topic, 0)
        enumerator.should be_a(MessageSetEnumerator)

        messages = enumerator.to_a
        message = messages.last.last
        expect(JSON.parse(message)).to eq({'a key' => 'a value'})
      end
    end
  end
end
