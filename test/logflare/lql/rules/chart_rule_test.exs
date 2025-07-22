defmodule Logflare.Lql.Rules.ChartRuleTest do
  use ExUnit.Case, async: true

  alias Logflare.Lql.Rules.ChartRule

  describe "__struct__" do
    test "creates struct with default values" do
      chart_rule = %ChartRule{}

      assert chart_rule.path == "timestamp"
      assert chart_rule.value_type == nil
      assert chart_rule.period == :minute
      assert chart_rule.aggregate == :count
    end

    test "creates struct with custom values" do
      chart_rule = %ChartRule{
        path: "metadata.requests",
        value_type: :integer,
        period: :hour,
        aggregate: :sum
      }

      assert chart_rule.path == "metadata.requests"
      assert chart_rule.value_type == :integer
      assert chart_rule.period == :hour
      assert chart_rule.aggregate == :sum
    end
  end

  describe "build_from_path/1" do
    test "builds changes map from path" do
      result = ChartRule.build_from_path("metadata.metric")
      assert is_map(result)
    end

    test "builds changes map from timestamp path" do
      result = ChartRule.build_from_path("timestamp")
      assert is_map(result)
    end

    test "builds changes map from empty path" do
      result = ChartRule.build_from_path("")
      assert is_map(result)
    end

    test "builds changes map from nil path" do
      result = ChartRule.build_from_path(nil)
      assert is_map(result)
    end
  end

  describe "Jason.Encoder" do
    test "encodes chart rule to JSON" do
      chart_rule = %ChartRule{
        path: "metadata.requests",
        value_type: :integer,
        period: :hour,
        aggregate: :sum
      }

      json = Jason.encode!(chart_rule)
      decoded = Jason.decode!(json)

      assert decoded["path"] == "metadata.requests"
      assert decoded["value_type"] == "integer"
      assert decoded["period"] == "hour"
      assert decoded["aggregate"] == "sum"
    end

    test "encodes chart rule with default values" do
      chart_rule = %ChartRule{}

      json = Jason.encode!(chart_rule)
      decoded = Jason.decode!(json)

      assert decoded["path"] == "timestamp"
      assert decoded["value_type"] == nil
      assert decoded["period"] == "minute"
      assert decoded["aggregate"] == "count"
    end
  end
end
