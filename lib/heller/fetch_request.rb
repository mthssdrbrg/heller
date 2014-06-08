# encoding: utf-8

module Heller
  class FetchRequest
    attr_reader :topic, :partition, :offset

    def initialize(*args)
      @topic, @partition, @offset = *args
    end
  end
end
