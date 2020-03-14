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

    def step(*args, &block)
      steps << build_callable(args, block)
      self
    end

    def filter(*args, &block)
      steps << Filter.new(build_callable(args, block))
      self
    end

    def reduce(*args, &block)
      steps << ItemReducer.new(build_callable(args, block))
      self
    end

    def pipeline(pipe = nil, &block)
      assert_callable!(pipe, block)

      pipe = Pipeline.new(&block) unless pipe
      steps << pipe
      self
    end

    def provides(key)
      @provided_key = key
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
      expected_keys
      provided_keys
      validate_dependent_keys!
      @steps.freeze
      freeze
    end

    def expected_keys
      @expected_keys ||= steps.flat_map(&:expected_keys).uniq
    end

    def provided_keys
      @provided_keys ||= ([@provided_key].compact + pipelines.flat_map(&:provided_keys)).uniq
    end

    protected

    attr_reader :provided_key

    def satisfied_by?(provided_keys)
      provided_keys.none? || expected_keys.none? || (expected_keys - provided_keys).none?
    end

    private

    attr_reader :steps

    def validate_dependent_keys!
      pipelines.each.reduce([]) do |pr_keys, pipe|
        if !pipe.satisfied_by?(pr_keys)
          raise DependencyError, "Child pipeline #{pipe} expects data keys #{pipe.expected_keys.join(', ')}, but is only given #{pr_keys.join(', ')}"
        end
        (pr_keys + pipe.provided_keys).uniq
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
      @pipelines ||= steps.find_all { |p| p.kind_of?(Pipeline) }
    end

    def build_callable(args, block)
      options = args.last.is_a?(Hash) ? args.pop : {}
      obj = assert_callable!(args.first, block)
      obj = CallableWithExpectedKeys.new(obj, options.fetch(:expects, [])) unless obj.respond_to?(:expected_keys)
      obj
    end

    def assert_callable!(obj, block)
      obj ||= block
      raise ArgumentError, 'this method expects a callable object or a proc' if obj.nil?
      raise ArgumentError, "argument #{obj.inspect} must respond to #call" unless obj.respond_to?(:call)

      obj
    end

    class CallableWithExpectedKeys < SimpleDelegator
      attr_reader :expected_keys

      def initialize(callable, expected_keys)
        super callable
        @expected_keys = Array(expected_keys)
      end
    end

    class Filter
      def initialize(callable)
        @callable = callable
      end

      def expected_keys
        @callable.expected_keys
      end

      def call(ctx)
        ctx.dataset.find_all { |item| @callable.call(item, ctx) }
      end
    end

    class ItemReducer
      def initialize(callable)
        @callable = callable
      end

      def expected_keys
        @callable.expected_keys
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
