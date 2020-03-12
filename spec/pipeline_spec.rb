# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Worktree::Pipeline do
  User = Struct.new(:name, :age, :age_diff)

  let(:users) do
    [
      User.new('Joe', 20),
      User.new('Ismael', 42),
      User.new('Fred', 34),
      User.new('Isabel', 45),
      User.new('Isambad', 39),
    ]
  end

  specify 'building a pipeline with steps as blocks' do
    pipe = described_class.new do |p|
      p.step &name_filter(/^I/)
    end

    result = pipe.run(users)
    expect(result.dataset.map(&:name)).to eq %w(Ismael Isabel Isambad)
  end

  specify 'building a pipeline with steps as callables' do
    pipe = described_class.new do |p|
      p.step name_filter(/^I/)
    end

    result = pipe.run(users)
    expect(result.dataset.map(&:name)).to eq %w(Ismael Isabel Isambad)
  end

  specify 'multiple steps' do
    pipe = described_class.new do |p|
      p.step name_filter(/^I/)
      p.step gte(:age, 40)
    end

    result = pipe.run(users)
    expect(result.dataset.map(&:name)).to eq %w(Ismael Isabel)
  end

  specify 'composing pipelines' do
    i_only = described_class.new do |p|
      p.step name_filter(/^I/)
    end

    pipe = described_class.new do |p|
      p.step i_only # we can use another pipeline as a step
      p.step gte(:age, 40)
    end

    result = pipe.run(users)
    expect(result.dataset.map(&:name)).to eq %w(Ismael Isabel)
  end

  describe '#filter' do
    it 'returns a boolean to reduce one item at a time' do
      pipe = described_class.new do |p|
        p.filter do |user, ctx|
          !!(user.name =~ /^I/)
        end
      end

      result = pipe.run(users)
      expect(result.dataset.map(&:name)).to eq %w(Ismael Isabel Isambad)
    end
  end

  describe '#reduce' do
    it 'raises ArgumentError if no callable nor block passed' do
      expect {
        described_class.new do |p|
          p.reduce
        end
      }.to raise_error(ArgumentError)
    end

    it 'returns new dataset, filters out when callable resolves to nil' do
      pipe = described_class.new do |p|
        p.reduce do |user, ctx|
          if user.age > 40
            User.new("Mr/s. #{user.name}", user.age)
          else
            nil
          end
        end
      end

      result = pipe.run(users)
      expect(result.dataset.map(&:name)).to eq ['Mr/s. Ismael', 'Mr/s. Isabel']
    end
  end

  describe '#pipeline' do
    it 'adds child pipeline instance' do
      i_only = described_class.new do |p|
        p.step name_filter(/^I/)
      end

      pipe = described_class.new do |p|
        p.pipeline i_only
      end

      result = pipe.run(users)
      expect(result.dataset.map(&:name)).to eq %w(Ismael Isabel Isambad)
    end

    it 'builds child pipeline from block' do
      pipe = described_class.new do |p|
        p.pipeline do |pp|
          pp.step name_filter(/^I/)
        end
      end

      result = pipe.run(users)
      expect(result.dataset.map(&:name)).to eq %w(Ismael Isabel Isambad)
    end
  end

  describe '#input_schema' do
    let(:pipe) do
      described_class.new do |p|
        p.input_schema do
          field(:sort).type(:string)
        end

        p.pipeline sub_pipe
      end
    end

    let(:sub_pipe) do
      described_class.new do |p|
        p.input_schema do
          field(:age).type(:integer)
        end

        p.pipeline do |pp|
          pp.input_schema do
            field(:name).type(:string).present
          end
        end
      end
    end

    it 'builds top-level schema from sub-schemas in pipeline tree' do
      expect(pipe.input_schema).to be_a(Parametric::Schema)
      expect(pipe.input_schema.fields.keys).to eq %i[sort age name]
    end

    it 'validates input against top-level schema' do
      result = pipe.run(users, input: {})
      expect(result.valid?).to be false
      expect(result.errors['$.name']).to eq(['is required'])
    end

    it 'bails out without running the pipeline' do
      result = pipe.run(users, input: {})
      expect(result.dataset.map(&:name)).to eq users.map(&:name)
    end
  end

  describe '#provides' do
    it 'sets computed data in custom keys in context' do
      pipe = described_class.new do |p|
        p.step name_filter(/^I/)

        p.pipeline do |pp|
          pp.provides :avg_age
          pp.step do |ctx|
            ctx.dataset.sum(&:age) / ctx.dataset.size.to_f
          end
        end

        p.pipeline do |pp|
          p.reduce do |user, ctx|
            user.age_diff = user.age - ctx[:avg_age]
            user
          end
        end
      end

      result = pipe.run(users)
      expect(result.dataset.map(&:name)).to eq %w(Ismael Isabel Isambad)
      expect(result.dataset.map(&:age_diff)).to eq([0.0, 3.0, -3.0])
      expect(result[:avg_age]).to eq((42 + 45 + 39) / 3.0)
    end
  end

  describe '#expects' do
    it 'raises a useful exception if #provides and #expects do not match' do
      expect {
        described_class.new do |p|
          p.provides :a_key
          p.pipeline do |pp|
            pp.provides :a_key
          end
          p.pipeline do |pp|
            pp.expects :another_key
          end
        end
      }.to raise_error(Worktree::DependencyError)

      expect {
        described_class.new do |p|
          p.provides :a_key
          p.pipeline do |pp|
            pp.expects :a_key
          end
          p.pipeline do |pp|
            pp.expects :a_key
            pp.pipeline do |ppp|
              ppp.expects :another_key
            end
          end
        end
      }.to raise_error(Worktree::DependencyError)
    end

    it 'is ok if all dependencies are met' do
      expect {
        described_class.new do |p|
          p.provides :a_key
          p.pipeline do |pp|
            # no explicit expectation means this pipelines accepts any keys
            pp.provides :a_key
          end
          p.pipeline do |pp|
            pp.expects :a_key
            pp.pipeline do |ppp|
              ppp.expects :a_key, :another_key
            end
          end
        end
      }.not_to raise_error
    end
  end

  private

  def name_filter(exp)
    Proc.new do |ctx|
      ctx.dataset.find_all { |user| user.name =~ exp }
    end
  end

  def gte(field, value)
    Proc.new do |ctx|
      ctx.dataset.find_all { |user| user[field] > value }
    end
  end
end
