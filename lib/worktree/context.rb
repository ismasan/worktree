# frozen_string_literal: true

require 'worktree/context'

module Worktree
  class Context
    attr_reader :set, :input, :errors

    def initialize(set, context: {}, input: {}, errors: {})
      @set, @context, @input, @errors = set, context, input, errors
    end

    def valid?
      errors.none?
    end

    def set!(new_set)
      @set = new_set
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
