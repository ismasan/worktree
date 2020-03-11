# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Worktree::Pipeline do
  User = Struct.new(:name, :age)

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

    result = pipe.call(users)
    expect(result.set.map(&:name)).to eq %w(Ismael Isabel Isambad)
  end

  specify 'building a pipeline with steps as callables' do
    pipe = described_class.new do |p|
      p.step name_filter(/^I/)
    end

    result = pipe.call(users)
    expect(result.set.map(&:name)).to eq %w(Ismael Isabel Isambad)
  end

  specify 'multiple steps' do
    pipe = described_class.new do |p|
      p.step name_filter(/^I/)
      p.step gte(:age, 40)
    end

    result = pipe.call(users)
    expect(result.set.map(&:name)).to eq %w(Ismael Isabel)
  end

  specify 'composing pipelines' do
    i_only = described_class.new do |p|
      p.step name_filter(/^I/)
    end

    pipe = described_class.new do |p|
      p.step i_only # we can use another pipeline as a step
      p.step gte(:age, 40)
    end

    result = pipe.call(users)
    expect(result.set.map(&:name)).to eq %w(Ismael Isabel)
  end

  describe '#filter' do
    it 'returns a boolean to reduce one item at a time' do
      pipe = described_class.new do |p|
        p.filter do |user, ctx|
          !!(user.name =~ /^I/)
        end
      end

      result = pipe.call(users)
      expect(result.set.map(&:name)).to eq %w(Ismael Isabel Isambad)
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

    it 'returns new set, filters out when callable resolves to nil' do
      pipe = described_class.new do |p|
        p.reduce do |user, ctx|
          if user.age > 40
            User.new("Mr/s. #{user.name}", user.age)
          else
            nil
          end
        end
      end

      result = pipe.call(users)
      expect(result.set.map(&:name)).to eq ['Mr/s. Ismael', 'Mr/s. Isabel']
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

      result = pipe.call(users)
      expect(result.set.map(&:name)).to eq %w(Ismael Isabel Isambad)
    end

    it 'builds child pipeline from block' do
      pipe = described_class.new do |p|
        p.pipeline do |pp|
          pp.step name_filter(/^I/)
        end
      end

      result = pipe.call(users)
      expect(result.set.map(&:name)).to eq %w(Ismael Isabel Isambad)
    end
  end

  describe '#input_schema' do
    it 'builds top-level schema from schemas in pipeline tree' do
      pipe = described_class.new do |p|
        p.input_schema do
          field(:sort).type(:string)
        end

        p.pipeline do |pp|
          pp.input_schema do
            field(:age).type(:integer)
          end

          pp.pipeline do |ppp|
            ppp.input_schema do
              field(:name).type(:string)
            end
          end
        end
      end

      expect(pipe.input_schema).to be_a(Parametric::Schema)
      expect(pipe.input_schema.fields.keys).to eq %i[sort age name]
    end
  end

  private

  def name_filter(exp)
    Proc.new do |ctx|
      ctx.set.find_all { |user| user.name =~ exp }
    end
  end

  def gte(field, value)
    Proc.new do |ctx|
      ctx.set.find_all { |user| user[field] > value }
    end
  end
end
