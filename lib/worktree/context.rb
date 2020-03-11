# frozen_string_literal: true

require 'worktree/context'

module Worktree
  class Context
    attr_reader :set, :input

    def initialize(set, context: {}, input: {})
      @set, @context, @input = set, context, input
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
