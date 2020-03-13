# frozen_string_literal: true

require 'parametric'
require 'worktree/context'

module Worktree
  Error = Class.new(StandardError)
  DependencyError = Class.new(Error)

  class Pipeline
    def initialize(&config)
      @steps = []
      @local_input_schema = nil
      @provided_key = nil
      @expected_keys = []

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
          elsif provided_key
            ctx[provided_key] = r
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

    def provides(key)
      @provided_key = key
      self
    end

    def expects(*keys)
      @expected_keys = keys
      self
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
      validate_dependent_keys!
      @steps.freeze
      freeze
    end

    def expected_keys
      (@expected_keys + pipelines.flat_map(&:expected_keys)).uniq
    end

    protected

    attr_reader :provided_key

    def satisfied_by?(provided_keys)
      return true if provided_keys.none?

      keys = expected_keys
      keys.none? || (keys - provided_keys).none?
    end

    private

    attr_reader :steps

    def validate_dependent_keys!
      pipelines.each.with_object([]) do |pipe, provided_keys|
        if !pipe.satisfied_by?(provided_keys)
          raise DependencyError, "Child pipeline #{pipe} expects data keys #{pipe.expected_keys.join(', ')}, but is only given #{provided_keys.join(', ')}"
        end
        provided_keys << pipe.provided_key
      end
    end

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
