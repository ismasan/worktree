# frozen_string_literal: true

require 'worktree/context'

module Worktree
  class Pipeline
    def initialize(&config)
      @steps = []
      if block_given?
        config.call(self)
        @steps.freeze
        freeze
      end
    end

    def call(ctx)
      ctx = Context.new(ctx) unless ctx.kind_of?(Context)
      catch(:stop) do
        steps.each do |stp|
          r = stp.call(ctx)
          if r.kind_of?(Context)
            ctx = r
          else
            ctx.set!(r)
          end
        end
      end

      ctx
    end

    def step(obj = nil, &block)
      steps << (obj || block)
    end

    def filter(obj = nil, &block)
      steps << Filter.new(obj || block)
    end

    private

    attr_reader :steps

    class Filter
      def initialize(callable)
        @callable = callable
      end

      def call(ctx)
        ctx.set.find_all { |item| @callable.call(item, ctx) }
      end
    end
  end
end
