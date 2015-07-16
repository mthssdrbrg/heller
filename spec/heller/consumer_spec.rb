# encoding: utf-8

require 'spec_helper'

module Heller
  describe Consumer do
    let(:consumer) do
      described_class.new('localhost:9092', consumer_impl: consumer_impl, client_id: 'spec-consumer')
    end

    let :consumer_impl do
      double(:consumer_impl)
    end

    let :consumer_spy do
      double(:consumer)
    end

    before do
      allow(consumer_impl).to receive(:new) do |*args|
        allow(consumer_spy).to receive(:client_id).and_return(args.last)
        consumer_spy
      end
      allow(consumer_spy).to receive(:fetch)
    end

    describe '#initialize' do
      it 'takes a connect string' do
        consumer = described_class.new('localhost:9092', consumer_impl: consumer_impl)
        expect(consumer_impl).to have_received(:new).with('localhost', 9092, anything, anything, anything)
      end

      it 'proxies arguments when creating the internal consumer' do
        consumer = described_class.new('localhost:9092', consumer_impl: consumer_impl)
        expect(consumer_impl).to have_received(:new).with('localhost', 9092, anything, anything, anything)
      end

      context 'when not given any options' do
        it 'fills in sane defaults for missing options' do
          consumer = described_class.new('localhost:9092', consumer_impl: consumer_impl)
          expect(consumer_impl).to have_received(:new).with('localhost', 9092, 30000, 65536, anything)
        end

        context 'client_id' do
          it 'makes some kind of attempt to generate a unique client id' do
            consumer = described_class.new('localhost:9092', consumer_impl: consumer_impl)
            expect(consumer.client_id).to match /heller\-consumer\-[a-f0-9]{8}\-[a-f0-9]{4}\-[a-f0-9]{4}\-[a-f0-9]{4}\-[a-f0-9]{12}/
          end
        end
      end

      context 'when given options' do
        it 'merges options with the defaults' do
          consumer = described_class.new('localhost:9092', consumer_impl: consumer_impl, timeout: 10, buffer_size: 11, client_id: 'hi')
          expect(consumer_impl).to have_received(:new).with('localhost', 9092, 10, 11, 'hi')
        end
      end
    end

    describe '#fetch' do
      let :topic do
        'spec'
      end

      let :partition do
        0
      end

      let :offset do
        0
      end

      context 'when given a single Heller::FetchRequest' do
        let :request do
          Heller::FetchRequest.new(topic, partition, offset)
        end

        it 'converts it to a Kafka::Api::FetchRequest' do
          expect(consumer_spy).to receive(:fetch).with(instance_of(Kafka::Api::FetchRequest))

          consumer.fetch(request)
        end

        it 'includes parameters from given Heller::FetchRequest' do
          expect(consumer_spy).to receive(:fetch) do |request|
            request_info = request.request_info
            expect(request_info.size).to eq(1)

            tuple = request_info.first
            expect(tuple._1.topic).to eq('spec')
            expect(tuple._1.partition).to eq(0)
            expect(tuple._2.offset).to eq(0)
          end

          consumer.fetch(request)
        end

        it 'returns a Heller::FetchResponse object' do
          expect(consumer.fetch(request)).to be_a(Heller::FetchResponse)
        end
      end

      context 'when given an array of Heller::FetchRequests' do
        it 'converts them to a Kafka::Api::FetchRequest' do
          expect(consumer_spy).to receive(:fetch) do |request|
            expect(request).to be_a(Kafka::Api::FetchRequest)
            expect(request.request_info.size).to eq(3)
          end

          requests = 3.times.map { |i| Heller::FetchRequest.new(topic, partition + i, offset) }
          consumer.fetch(requests)
        end
      end

      context 'fetch options' do
        let :fetch_request do
          Heller::FetchRequest.new(topic, partition, offset)
        end

        it 'sets a default fetch size' do
          expect(consumer_spy).to receive(:fetch) do |request|
            tuple = request.request_info.first
            expect(tuple._2.fetch_size).to eq(1024 * 1024)
          end

          consumer.fetch(fetch_request)
        end

        it 'allows fetch size to be overridden' do
          expect(consumer_spy).to receive(:fetch) do |request|
            tuple = request.request_info.first
            expect(tuple._2.fetch_size).to eq(1024)
          end

          consumer.fetch(fetch_request, 1024)
        end

        it 'includes the client_id' do
          expect(consumer_spy).to receive(:fetch) do |request|
            expect(request.client_id).to eq('spec-consumer')
          end

          consumer.fetch(fetch_request)
        end

        it 'includes max_wait if given when the consumer was created' do
          consumer = described_class.new('localhost:9092', consumer_impl: consumer_impl, client_id: 'spec-consumer', max_wait: 1)

          expect(consumer_spy).to receive(:fetch) do |request|
            expect(request.max_wait).to eq(1)
          end

          consumer.fetch(fetch_request)
        end

        it 'includes min_bytes if given when the consumer was created' do
          consumer = described_class.new('localhost:9092', consumer_impl: consumer_impl, client_id: 'spec-consumer', min_bytes: 1024)

          expect(consumer_spy).to receive(:fetch) do |request|
            expect(request.min_bytes).to eq(1024)
          end

          consumer.fetch(fetch_request)
        end
      end
    end

    describe '#offsets_before' do
      before do
        allow(consumer_spy).to receive(:get_offsets_before)
      end

      let :topic do
        'spec'
      end

      let :partition do
        0
      end

      let :time do
        Time.utc(2013, 7, 20)
      end

      let :offset_request do
        Heller::OffsetRequest.new(topic, partition, time)
      end

      it 'sends an OffsetRequest using #get_offsets_before' do
        expect(consumer_spy).to receive(:get_offsets_before) do |request|
          expect(request).to be_a(Kafka::JavaApi::OffsetRequest)
        end

        consumer.offsets_before(offset_request)
      end

      it 'returns a Heller::OffsetResponse' do
        expect(consumer.offsets_before(offset_request)).to be_a(Heller::OffsetResponse)
      end

      it 'includes client_id' do
        expect(consumer_spy).to receive(:get_offsets_before) do |request|
          expect(request.underlying.client_id).not_to be_nil
        end

        consumer.offsets_before(offset_request)
      end

      it 'accepts ints instead of Time objects' do
        expect(consumer_spy).to receive(:get_offsets_before) do |request|
          request_info = request.underlying.request_info
          expect(request.underlying.request_info.values.first.time).to eq(0)
          expect(request_info.values.first.time).to eq(0)
        end

        offset_request = Heller::OffsetRequest.new(topic, partition, 0)
        consumer.offsets_before(offset_request)
      end

      context 'maximum number of offsets to fetch' do
        it 'defaults to 1' do
          expect(consumer_spy).to receive(:get_offsets_before) do |request|
            request_info = request.underlying.request_info
            expect(request_info.values.first.max_num_offsets).to eq(1)
          end

          consumer.offsets_before(Heller::OffsetRequest.new('spec', 0, 0))
        end

        it 'is overridable' do
          expect(consumer_spy).to receive(:get_offsets_before) do |request|
            request_info = request.underlying.request_info
            expect(request_info.values.first.max_num_offsets).to eq(10)
          end

          consumer.offsets_before(Heller::OffsetRequest.new('spec', 0, 0, 10))
        end
      end
    end

    describe '#earliest_offset' do
      let :fake_offset_response do
        double(:offset_response)
      end

      before do
        allow(consumer_spy).to receive(:get_offsets_before).and_return(fake_offset_response)
        allow(fake_offset_response).to receive(:offsets).with('spec', 0).and_return([0, 1, 2])
      end

      it 'sends an OffsetRequest with the magic value for \'earliest\' offset' do
        expect(consumer_spy).to receive(:get_offsets_before) do |request|
          request_info = request.underlying.request_info
          expect(request_info.values.first.time).to eq(-2)

          fake_offset_response
        end

        consumer.earliest_offset('spec', 0)
      end

      it 'returns a single offset' do
        expect(consumer.earliest_offset('spec', 0)).to eq(0)
      end

      it 'fetches only one offset' do
        expect(consumer_spy).to receive(:get_offsets_before) do |request|
          request_info = request.underlying.request_info
          expect(request_info.values.first.max_num_offsets).to eq(1)

          fake_offset_response
        end

        consumer.earliest_offset('spec', 0)
      end
    end

    describe '#latest_offset' do
      let :fake_offset_response do
        double(:offset_response)
      end

      before do
        allow(fake_offset_response).to receive(:offsets).with('spec', 0).and_return([0, 1, 2])
        allow(consumer_spy).to receive(:get_offsets_before).and_return(fake_offset_response)
      end

      it 'sends an OffsetRequest with the magic value for \'latest\' offset' do
        expect(consumer_spy).to receive(:get_offsets_before) do |request|
          request_info = request.underlying.request_info
          expect(request_info.values.first.time).to eq(-1)

          fake_offset_response
        end

        consumer.latest_offset('spec', 0)
      end

      it 'returns a single offset' do
        expect(consumer.latest_offset('spec', 0)).to eq(2)
      end

      it 'fetches only one offset' do
        expect(consumer_spy).to receive(:get_offsets_before) do |request|
          request_info = request.underlying.request_info
          expect(request_info.values.first.max_num_offsets).to eq(1)

          fake_offset_response
        end

        consumer.latest_offset('spec', 0)
      end
    end

    describe '#metadata' do
      before do
        allow(consumer_spy).to receive(:send)
      end

      context 'given a list of topics' do
        it 'sends a TopicMetadataRequest' do
          consumer.metadata(['topic1', 'topic2'])
          expect(consumer_spy).to have_received(:send) do |request|
            expect(request).to be_a(Kafka::JavaApi::TopicMetadataRequest)
            expect(request.topics.to_a).to eql(['topic1', 'topic2'])
          end
        end

        it 'returns a Heller::TopicMetadataResponse' do
          expect(consumer.metadata(['topic1', 'topic2'])).to be_a(Heller::TopicMetadataResponse)
        end
      end

      context 'given an empty list' do
        it 'sends a TopicMetadataRequest' do
          consumer.metadata([])
          expect(consumer_spy).to have_received(:send) do |request|
            expect(request.topics.to_a).to eq([])
          end
        end
      end

      context 'given no arguments' do
        it 'sends a TopicMetadataRequest with an empty list of topics' do
          consumer.metadata
          expect(consumer_spy).to have_received(:send) do |request|
            expect(request.topics.to_a).to eq([])
          end
        end
      end

      it 'is aliased as #topic_metadata' do
        consumer.topic_metadata
        expect(consumer_spy).to have_received(:send).with(an_instance_of(Kafka::JavaApi::TopicMetadataRequest))
      end
    end

    context '#disconnect' do
      before do
        allow(consumer_spy).to receive(:close)
      end

      it 'calls #close on the underlying consumer' do
        consumer.disconnect

        expect(consumer_spy).to have_received(:close)
      end

      it 'removes any metrics associated with the underlying consumer' do
        consumer = described_class.new('localhost:9092', client_id: 'metrics-removal')
        before = Java::ComYammerMetrics::Metrics.default_registry.all_metrics.keys
        before.select! { |mn| mn.mbean_name.match(/.*clientId=metrics-removal.*/) }
        expect(before).to_not be_empty
        consumer.close
        after = Java::ComYammerMetrics::Metrics.default_registry.all_metrics.keys
        after.select! { |mn| mn.mbean_name.match(/.*clientId=metrics-removal.*/) }
        expect(after).to be_empty
      end

      it 'is aliased to #close' do
        consumer.close

        expect(consumer_spy).to have_received(:close)
      end

      it 'returns nil' do
        expect(consumer.close).to be_nil
      end

      it 'is okay to call multiple times' do
        consumer = described_class.new('localhost:9092', client_id: 'close-multiple')
        3.times { consumer.close }
      end
    end
  end
end
