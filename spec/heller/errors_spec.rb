# encoding: utf-8

module Heller
  describe Errors do
    describe '.error_for' do
      it 'calls .exception_for' do
        expect(Errors).to receive(:exception_for).with(0)

        Errors.error_for(0)
      end
    end
  end
end
