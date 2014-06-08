# encoding: utf-8

require 'spec_helper'


describe 'End-to-end communication using ZookeeperConsumer' do
  let :producer do
    Heller::Producer.new('localhost:9092', client_id: 'test-producer', batch_size: 1, num_retries: 10, retry_backoff: 1000)
  end

  let :consumer do
    Heller::ZookeeperConsumer.new({
      zk_connect: 'localhost:2181',
      group_id: 'test',
      auto_reset_offset: :smallest,
      timeout: 1000,
      fetch_wait_max: 1000,
    })
  end

  let :decoder do
    Kafka::Serializer::StringDecoder.new(nil)
  end

  let :consumed do
    []
  end

  before do
    messages.each do |message|
      producer.push(Heller::Message.new(topic, message))
    end
  end

  after do
    producer.close
    consumer.close
  end

  describe '#create_streams' do
    let :topic do
      "spec-create-streams-#{Time.now.to_i.to_s(36).upcase}"
    end

    let :messages do
      %w[first-stream-message second-stream-message third-stream-message]
    end

    it 'can actually consume messages' do
      streams_map = consumer.create_streams({topic => 1}, {key_decoder: decoder, value_decoder: decoder})
      stream = streams_map[topic].first
      iterator = stream.iterator

      messages.size.times do
        consumed << iterator.next.message
      end

      expect(consumed).to eq(messages)
    end
  end

  describe '#create_streams_by_filter' do
    let :topic do
      "spec-with-filter-#{time_component}"
    end

    let :time_component do
      Time.now.to_i.to_s(36).upcase
    end

    let :messages do
      %w[first-filter-message second-filter-message third-filter-message]
    end

    it 'can actually consume messages' do
      streams = consumer.create_streams_by_filter(".+-#{time_component}", 1, {key_decoder: decoder, value_decoder: decoder})
      iterator = streams.first.iterator

      messages.size.times do
        consumed << iterator.next.message
      end

      expect(consumed).to eq(messages)
    end
  end
end
