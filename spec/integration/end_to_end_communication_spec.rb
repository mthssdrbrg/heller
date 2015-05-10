# encoding: utf-8

require 'spec_helper'


describe 'End-to-end communication' do
  let :producer do
    Heller::Producer.new('localhost:9092', producer_options)
  end

  let :consumer do
    Heller::Consumer.new('localhost:9092', client_id: 'spec-consumer')
  end

  let :producer_options do
    {
      client_id: 'spec-producer',
      batch_size: 1,
      num_retries: 10,
      retry_backoff: 1000,
    }
  end

  after do
    producer.close
    consumer.close
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
        consumer.fetch(Heller::FetchRequest.new(topic, 0, 0))
      end

      let :enumerator do
        fetch_response.messages(topic, 0)
      end

      context 'simple string messages' do
        let :topic do
          "spec-simple-string-#{Time.now.to_i.to_s(36)}"
        end

        before do
          producer.push(Heller::Message.new(topic, 'simple string message'))
        end

        it 'is no big deal' do
          expect(enumerator).to be_a(Heller::MessageSetEnumerator)

          messages = enumerator.to_a
          expect(messages.size).to eq(1)

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
          expect(enumerator).to be_a(Heller::MessageSetEnumerator)

          messages = enumerator.to_a
          expect(messages.size).to eq(1)

          offset, message = messages.last
          expect(offset).to be_zero
          expect(JSON.parse(message)).to eq({'a key' => 'a value'})
        end
      end

      context 'with Snappy compression' do
        let :producer_options do
          super.merge({
            client_id: 'spec-producer-snappy',
            compression: :snappy,
          })
        end

        context 'simple string messages' do
          let :topic do
            "spec-snappy-simple-string-#{Time.now.to_i.to_s(36)}"
          end

          before do
            producer.push(Heller::Message.new(topic, 'simple string message'))
          end

          it 'is no big deal' do
            expect(enumerator).to be_a(Heller::MessageSetEnumerator)

            messages = enumerator.to_a
            expect(messages.size).to eq(1)

            offset, message = messages.last
            expect(offset).to be_zero
            expect(message).to eq('simple string message')
          end
        end

        context 'JSON serialized hashes' do
          let :topic do
            "spec-snappy-json-hash-#{Time.now.to_i.to_s(36)}"
          end

          before do
            producer.push(Heller::Message.new(topic, {'a key' => 'a value'}.to_json))
          end

          it 'is no big deal' do
            expect(enumerator).to be_a(Heller::MessageSetEnumerator)

            messages = enumerator.to_a
            expect(messages.size).to eq(1)

            offset, message = messages.last
            expect(offset).to be_zero
            expect(JSON.parse(message)).to eq({'a key' => 'a value'})
          end
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

          expect(metadata.size).to eq(1)

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

          expect(metadata.size).to eq(3)

          topics.zip(metadata).each do |topic, metadata|
            expect(metadata.topic).to eq(topic)
          end
        end
      end

      describe '#leader_for' do
        let :topics do
          [1, 2, 3].map { |i| "spec-metadata-leader-for-#{i}-#{Time.now.to_i.to_s(36)}" }
        end

        let :response do
          consumer.metadata(topics)
        end

        context 'for existing topic-partition combinations' do
          it 'returns the correct leader for each topic-partition combination' do
            topics.each do |topic|
              leader = response.leader_for(topic, 0)

              expect(leader.connection_string).to match(/[a-z0-9\-\.]+:9092/i)
            end
          end
        end

        context 'for non-existing topic-partition combinations' do
          it 'raises NoSuchTopicPartitionCombinationError' do
            expect { response.leader_for('non-existent', 0) }.to raise_error(Heller::NoSuchTopicPartitionCombinationError)
          end
        end
      end

      describe '#isr_for' do
        let :topics do
          [1, 2, 3].map { |i| "spec-metadata-isr-for-#{i}-#{Time.now.to_i.to_s(36)}" }
        end

        let :response do
          consumer.metadata(topics)
        end

        context 'for existing topic-partition combinations' do
          it 'returns the correct in sync replicas for each topic-partition combination' do
            topics.each do |topic|
              isr = response.isr_for(topic, 0)

              expect(isr.size).to eq(1)

              replica = isr.first

              expect(replica.connection_string).to match(/[a-z0-9\-\.]+:9092/i)
            end
          end
        end

        context 'for non-existing topic-partition combinations' do
          it 'raises NoSuchTopicPartitionCombinationError' do
            expect { response.isr_for('non-existent', 0) }.to raise_error(Heller::NoSuchTopicPartitionCombinationError)
          end
        end
      end
    end

    describe '#offsets_before' do
      let :topic do
        "spec-offsets-before-#{Time.now.to_i.to_s(36)}"
      end

      let :requests do
        Heller::OffsetRequest.new(topic, 0, (Time.now + 1).to_i * 1000, 3)
      end

      let :response do
        consumer.offsets_before(requests)
      end

      before do
        3.times { producer.push(Heller::Message.new(topic, 'offsets request message')) }
      end

      context 'for existing topic-partition combination(s)' do
        it 'returns the expected offsets' do
          offsets = response.offsets(topic, 0)

          expect(offsets.size).to eq(2)
          expect(offsets.first).to be > offsets.last
        end
      end

      context 'for non-existing topic-partition combination(s)' do
        it 'raises NoSuchTopicPartitionCombinationError' do
          expect { response.offsets('non-existent', 0) }.to raise_error(Heller::NoSuchTopicPartitionCombinationError)
        end
      end
    end

    describe '#earliest_offset' do
      let :topic do
        "spec-earliest-offset-#{Time.now.to_i.to_s(36)}"
      end

      let :response do
        consumer.earliest_offset(topic, 0)
      end

      before do
        3.times { producer.push(Heller::Message.new(topic, 'offsets request message')) }
      end

      it 'returns the earliest offset' do
        expect(response).to be_zero
      end
    end

    describe '#latest_offset' do
      let :topic do
        "spec-latest-offset-#{Time.now.to_i.to_s(36)}"
      end

      before do
        3.times { producer.push(Heller::Message.new(topic, 'offsets request message')) }
      end

      it 'returns the latest offset' do
        response = consumer.latest_offset(topic, 0)
        expect(response).to eq(3)
      end
    end
  end
end
