# encoding: utf-8

module Heller
  Errors = Kafka::Errors::ErrorMapping

  class Errors
    self.singleton_class.send(:alias_method, :error_for, :exception_for)
  end
end
