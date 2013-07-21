module Fakers
  class FakeFetchResponse < Struct.new(:topic, :partition, :messages)
    def message_set(topic, partition)
      if topic == self.topic && partition == self.partition
        messages
      end
    end
  end

  FakeMessageAndOffset = Struct.new(:message, :offset)
  FakeMessage = Struct.new(:payload)

  class FakePayload
    def initialize(payload)
      @payload = payload.to_java_bytes
    end

    def limit
      @payload.size
    end

    def get(buffer)
      @payload.each_with_index do |b, i|
        buffer[i] = b
      end
    end
  end

  def create_fake_message(payload)
    FakeMessage.new(FakePayload.new(payload))
  end

  def create_fake_fetch_response(*payloads)
    message_set = create_fake_message_set(*payloads)
    FakeFetchResponse.new('spec', 0, message_set)
  end

  FakeMessageSet = Struct.new(:iterator)

  class FakeIterator < Struct.new(:messages)
    def next
      messages.shift
    end

    def any?
      messages.any?
    end
  end

  def create_fake_message_set(*payloads)
    messages = payloads.each_with_index.map do |payload, index|
      message = create_fake_message(payload)
      FakeMessageAndOffset.new(message, index)
    end

    FakeMessageSet.new(FakeIterator.new(messages))
  end
end
