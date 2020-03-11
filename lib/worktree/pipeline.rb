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
      steps << resolve_callable!(obj, block)
      self
    end

    def filter(obj = nil, &block)
      steps << Filter.new(resolve_callable!(obj, block))
      self
    end

    def reduce(obj = nil, &block)
      steps << ItemReducer.new(resolve_callable!(obj, block))
      self
    end

    def pipeline(pipe = nil, &block)
      resolve_callable!(pipe, block)

      pipe = Pipeline.new(&block) unless pipe
      step pipe
    end

    private

    attr_reader :steps

    def resolve_callable!(obj, block)
      obj ||= block
      raise ArgumentError, 'this method expects a callable object or a proc' if obj.nil?
      raise ArgumentError, "argument #{obj.inspect} must respond to #call" unless obj.respond_to?(:call)

      obj
    end

    class Filter
      def initialize(callable)
        @callable = callable
      end

      def call(ctx)
        ctx.set.find_all { |item| @callable.call(item, ctx) }
      end
    end

    class ItemReducer
      def initialize(callable)
        @callable = callable
      end

      def call(ctx)
        ctx.set.each.with_object([]) do |item, ret|
          r = @callable.call(item, ctx)
          ret << r unless r.nil?
        end
      end
    end
  end
end
