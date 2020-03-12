# frozen_string_literal: true

require 'worktree/context'

module Worktree
  class Context
    attr_reader :dataset, :input, :errors

    def initialize(dataset, context: {}, input: {}, errors: {})
      @dataset, @context, @input, @errors = dataset, context, input, errors
    end

    def valid?
      errors.none?
    end

    def set!(new_set)
      @dataset = new_set
      self
    end

    def []=(key, value)
      @context[key] = value
    end

    def [](key)
      @context[key]
    end
  end
end
