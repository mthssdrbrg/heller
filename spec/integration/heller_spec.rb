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

    context 'Producer' do
      context 'without an explicit key' do
        let :topic do
          "spec-without-explicit-key-#{Time.now.to_i.to_s(36)}"
        end

        it 'is able to push messages' do
          expect { producer.push(Heller::Message.new(topic, 'simple string message')) }.not_to raise_error
        end
      end

      context 'with an explicit key' do
        let :topic do
          "spec-with-explicit-key-#{Time.now.to_i.to_s(36)}"
        end

        it 'is able to push messages' do
          expect { producer.push(Heller::Message.new(topic, 'simple string message', 'some-key')) }.not_to raise_error
        end
      end
    end

    context 'Consumer' do
      describe '#fetch' do
        let :fetch_response do
          consumer.fetch(FetchRequest.new(topic, 0, 0))
        end

        context 'simple string messages' do
          let :topic do
            "spec-simple-string-#{Time.now.to_i.to_s(36)}"
          end

          before do
            producer.push(Heller::Message.new(topic, 'simple string message'))
          end

          it 'is no big deal' do
            enumerator = fetch_response.messages(topic, 0)
            enumerator.should be_a(MessageSetEnumerator)

            messages = enumerator.to_a
            expect(messages).to have(1).item

            offset, message = messages.last
            expect(offset).to be_zero
            expect(message).to eq('simple string message')
          end
        end

        context 'JSON serialized hashes' do
          let :topic do
            "spec-json-hash-#{Time.now.to_i.to_s(36)}"
          end

          before do
            producer.push(Heller::Message.new(topic, {'a key' => 'a value'}.to_json))
          end

          it 'is no big deal' do
            enumerator = fetch_response.messages(topic, 0)
            enumerator.should be_a(MessageSetEnumerator)

            messages = enumerator.to_a
            expect(messages).to have(1).item

            offset, message = messages.last
            expect(offset).to be_zero
            expect(JSON.parse(message)).to eq({'a key' => 'a value'})
          end
        end
      end

      describe '#metadata' do
        before do
          topics.each { |topic| producer.push(Heller::Message.new(topic, 'metadata request message')) }
        end

        context 'when given a single topic' do
          let :topics do
            ["spec-single-metadata-topic-#{Time.now.to_i.to_s(36)}"]
          end

          it 'returns metadata about given topic' do
            response = consumer.metadata(topics)
            metadata = response.metadata

            expect(metadata).to have(1).item

            topic_metadata = metadata.first
            expect(topic_metadata.topic).to eq(topics.first)
          end
        end

        context 'when given several topics' do
          let :topics do
            [1, 2, 3].map { |i| "spec-multiple-metadata-topics-#{i}-#{Time.now.to_i.to_s(36)}" }
          end

          it 'returns metadata about given topics' do
            response = consumer.metadata(topics)
            metadata = response.metadata

            expect(metadata).to have(3).item

            topics.zip(metadata).each do |topic, metadata|
              expect(metadata.topic).to eq(topic)
            end
          end
        end
      end

      describe '#offsets_before' do
        pending
      end

      describe '#earliest_offset' do
        pending
      end

      describe '#latest_offset' do
        pending
      end
    end
  end
end
