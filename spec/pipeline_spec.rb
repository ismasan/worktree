# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Worktree::Pipeline do
  User = Struct.new(:name)

  specify 'building a pipeline' do
    pipe = described_class.new do |p|
      p.step do |ctx|
        ctx.set.find_all{ |item| item[:name] =~ /^I/ }
      end
    end

    result = pipe.call([User.new('Joe'), User.new('Ismael'), User.new('Fred'), User.new('Isabel')])
    expect(result.set.map(&:name)).to eq %w(Ismael Isabel)
  end
end
