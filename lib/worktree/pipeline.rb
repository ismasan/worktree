# frozen_string_literal: true

require 'parametric'
require 'worktree/context'

module Worktree
  class Pipeline
    def initialize(&config)
      @steps = []
      @local_input_schema = nil

      if block_given?
        config.call(self)
        build!
      end
    end

    def run(dataset, input: {})
      validated = input_schema.resolve(input)
      ctx = Context.new(dataset, input: validated.output, errors: validated.errors)
      return ctx unless validated.valid?

      call(ctx)
    end

    def call(ctx)
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

    def input_schema(obj = nil, &block)
      if obj
        @local_input_schema = obj
      elsif block_given?
        @local_input_schema = Parametric::Schema.new(&block)
      else
        @input_schema ||= build_schema(@local_input_schema, :input_schema)
      end
    end

    def build!
      input_schema # eagerly build schema
      @steps.freeze
      freeze
    end

    private

    attr_reader :steps

    def build_schema(this_schema, schema_name)
      sh = this_schema || Parametric::Schema.new
      pipelines.each do |p|
        sh = sh.merge(p.public_send(schema_name))
      end

      sh
    end

    def pipelines
      steps.find_all { |p| p.kind_of?(Pipeline) }
    end

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
        ctx.dataset.find_all { |item| @callable.call(item, ctx) }
      end
    end

    class ItemReducer
      def initialize(callable)
        @callable = callable
      end

      def call(ctx)
        ctx.dataset.each.with_object([]) do |item, ret|
          r = @callable.call(item, ctx)
          ret << r unless r.nil?
        end
      end
    end
  end
end
