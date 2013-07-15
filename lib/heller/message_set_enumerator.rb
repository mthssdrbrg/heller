# encoding: utf-8

module Heller
  class MessageSetEnumerator
    include Enumerable

    def initialize(message_set, decoder)
      @message_set, @decoder = message_set, decoder
    end

    def each
      loop do
        yield self.next
      end
    end

    def next
      if @message_set.any?
        item = @message_set.shift
        offset, payload = item.offset, item.message.payload
        [offset, decode(payload)]
      else
        raise StopIteration
      end
    end

    private

    def decode(payload)
      bytes = Java::byte[payload.limit].new
      payload.get(bytes)
      @decoder.from_bytes(bytes)
    end
  end
end
