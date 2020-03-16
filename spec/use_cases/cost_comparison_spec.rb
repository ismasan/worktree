# frozen_string_literal: true

require 'spec_helper'

RSpec.describe 'Computing and comparing product cost' do
  let(:product) { Struct.new(:slug, :monthly_cost, keyword_init: true) }
  let(:result) { Struct.new(:slug, :monthly_cost, :total_cost, :savings, keyword_init: true) }
  let(:dataset) do
    [
      product.new(slug: 'p1', monthly_cost: 10),
      product.new(slug: 'p2', monthly_cost: 15),
      product.new(slug: 'p3', monthly_cost: 20),
      product.new(slug: 'p4', monthly_cost: 25),
      product.new(slug: 'p5', monthly_cost: 30)
    ]
  end

  let(:pipeline) do
    Worktree::Pipeline.new do |p|
      # Find current product
      p.pipeline do |c|
        # The results of this pipeline are set into the :current key in the output object
        # The next pipeline can then use that data to compare current product to other products.
        c.provides :current
        c.step current_product_pipeline
      end

      # Filter all products and compare against current product
      p.pipeline do |c|
        # Exclude current product
        c.filter expects: :current do |item, ctx|
          item.slug != ctx[:current].slug
        end

        # Compute total cost
        c.step product_cost_pipeline

        # Compute savings
        c.reduce calculate_savings, expects: :current
      end
    end
  end

  let(:calculate_savings) do
    Proc.new do |pr, ctx|
      savings = pr.total_cost - ctx[:current].total_cost
      result.new(
        slug: pr.slug,
        monthly_cost: pr.monthly_cost,
        total_cost: pr.total_cost,
        savings: savings
      )
    end
  end

  let(:current_product_pipeline) do
    Worktree::Pipeline.new do |p|
      # The input fields required by this pipeline
      p.input_schema do
        field(:current_product_slug).type(:string).present
      end

      # Find the product by slug
      p.filter do |pr, ctx|
        pr.slug == ctx.input[:current_product_slug]
      end

      # Use a shared sub-pipeline to calculate total product price
      p.pipeline product_cost_pipeline

      # We only care about the first result (there should only be one)
      p.step do |ctx|
        ctx.dataset.first
      end
    end
  end

  let(:product_cost_pipeline) do
    Worktree::Pipeline.new do |p|
      p.input_schema do
        field(:months).type(:integer).default(1)
      end

      p.reduce do |pr, ctx|
        result.new(
          slug: pr.slug,
          monthly_cost: pr.monthly_cost,
          total_cost: pr.monthly_cost * ctx.input[:months],
          savings: 0
        )
      end
    end
  end

  it 'finds and reduces the current product' do
    r = pipeline.run(dataset, input: { current_product_slug: 'p2', months: 2 })
    expect(r[:current].slug).to eq 'p2'
    expect(r[:current].total_cost).to eq 30
    expect(r[:current].savings).to eq 0
  end

  it 'filters all other products and compares their total costs relative to the current product' do
    r = pipeline.run(dataset, input: { current_product_slug: 'p2', months: 2 })
    expect(r.dataset.map(&:slug)).to eq %w(p1 p3 p4 p5)
    expect(r.dataset.map(&:total_cost)).to eq [20, 40, 50, 60]
    expect(r.dataset.map(&:savings)).to eq [-10, 10, 20, 30]
  end
end
