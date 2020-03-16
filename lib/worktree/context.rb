# frozen_string_literal: true

require 'worktree/context'

module Worktree
  class Context
    attr_reader :dataset, :input, :errors

    def initialize(dataset, data: {}, input: {}, errors: {})
      @dataset = dataset
      @data = data
      @input = input
      @errors = errors
    end

    def to_h
      @data
    end

    def valid?
      errors.none?
    end

    def copy(set, hash = {})
      self.class.new(
        set,
        data: @data.merge(hash),
        input: input,
        errors: errors
      )
    end

    def []=(key, value)
      @data[key] = value
      self
    end

    def [](key)
      @data[key]
    end
  end
end
