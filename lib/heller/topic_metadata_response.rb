# encoding: utf-8

module Heller
  class TopicMetadataResponse
    def initialize(underlying)
      @underlying = underlying

      @cache = Hash.new.tap do |h|
        CACHES.each do |type|
          h[type] = Hash.new({})
        end
      end
    end

    def each(&block)
      metadata.each do |topic_metadata|
        topic_metadata.partitions_metadata.each do |partition_metadata|
          yield topic_metadata.topic, partition_metadata
        end
      end
    end

    def metadata
      @underlying.topics_metadata
    end

    def leader_for(topic, partition)
      with_cache(:leader, topic, partition)
    end

    def isr_for(topic, partition)
      with_cache(:isr, topic, partition)
    end
    alias_method :in_sync_replicas_for, :isr_for

    private

    CACHES = [:leader, :isr].freeze

    def with_cache(type, topic, partition)
      return @cache[type][topic][partition] if @cache[type][topic][partition]

      partition_metadata = locate_partition_metadata(topic, partition)

      if partition_metadata
        @cache[type][topic][partition] = partition_metadata.send(type)
      else
        raise NoSuchTopicPartitionCombinationError, "Cannot find (#{topic}:#{partition}) combination"
      end
    end

    def locate_partition_metadata(topic, partition)
      metadata.each do |tm|
        if tm.topic == topic
          tm.partitions_metadata.each do |pm|
            return pm if pm.partition_id == partition
          end
        end
      end

      nil
    end
  end
end
