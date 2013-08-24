# encoding: utf-8

require 'spec_helper'

module Heller
  describe TopicMetadataResponse do
    let :response do
      described_class.new(underlying)
    end

    let :underlying do
      double(:topic_metadata_response, topics_metadata: [fake_topics_metadata])
    end

    let :fake_topics_metadata do
      double(:topics_metadata)
    end

    describe '#each' do
      let :fake_partition_metadata do
        double(:partition_metadata)
      end

      before do
        fake_topics_metadata.stub(:topic).and_return('spec')
        fake_topics_metadata.stub(:partitions_metadata).and_return([fake_partition_metadata])
      end

      it 'yields topic and partition_metadata' do
        yielded = []
        response.each { |topic, meta| yielded << [topic, meta] }
        expect(yielded.flatten).to eq(['spec', fake_partition_metadata])
      end
    end

    describe '#metadata' do
      before do
        underlying.stub(:topics_metadata)
      end

      it 'returns #topics_metadata' do
        response.metadata

        expect(underlying).to have_received(:topics_metadata).once
      end
    end

    describe '#leader_for' do
      context 'given a topic-partition combination that exists' do
        let :fake_partition_metadata do
          double(:partition_metadata)
        end

        before do
          fake_topics_metadata.stub(:topic).and_return('spec')
          fake_topics_metadata.stub(:partitions_metadata).and_return([fake_partition_metadata])
          fake_partition_metadata.stub(:partition_id).and_return(0)
          fake_partition_metadata.stub(:leader).and_return('a non-nil value')
        end

        it 'returns the leader' do
          expect(response.leader_for('spec', 0)).not_to be_nil

          expect(fake_partition_metadata).to have_received(:leader)
        end

        it 'caches the result' do
          2.times { expect(response.leader_for('spec', 0)).not_to be_nil }

          expect(fake_partition_metadata).to have_received(:leader).once
        end
      end

      context 'given a topic-partition combination that does not exist' do
        before do
          fake_topics_metadata.stub(:topic).and_return('not-spec')
        end

        it 'raises NoSuchTopicPartitionCombinationError' do
          expect { response.leader_for('non-existent', 1) }.to raise_error(NoSuchTopicPartitionCombinationError)
        end
      end
    end

    describe '#isr_for' do
      context 'given a topic-partition combination that exists' do
        let :fake_partition_metadata do
          double(:partition_metadata)
        end

        before do
          fake_topics_metadata.stub(:topic).and_return('spec')
          fake_topics_metadata.stub(:partitions_metadata).and_return([fake_partition_metadata])
          fake_partition_metadata.stub(:partition_id).and_return(0)
          fake_partition_metadata.stub(:isr).and_return(['a non-nil value'])
        end

        it 'returns in sync replicas' do
          expect(response.isr_for('spec', 0)).not_to be_nil

          expect(fake_partition_metadata).to have_received(:isr)
        end

        it 'caches the result' do
          2.times { expect(response.isr_for('spec', 0)).not_to be_nil }

          expect(fake_partition_metadata).to have_received(:isr).once
        end
      end

      context 'given a topic-partition combination that does not exist' do
        before do
          fake_topics_metadata.stub(:topic).and_return('not-spec')
        end

        it 'raises NoSuchTopicPartitionCombinationError' do
          expect { response.isr_for('non-existent', 1) }.to raise_error(NoSuchTopicPartitionCombinationError)
        end
      end
    end
  end
end
