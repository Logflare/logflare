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
    test "builds ChartRule struct from path" do
      result = ChartRule.build_from_path("metadata.metric")
      assert %ChartRule{} = result
      assert result.path == "metadata.metric"
    end

    test "builds ChartRule struct from timestamp path" do
      result = ChartRule.build_from_path("timestamp")
      assert %ChartRule{} = result
      assert result.path == "timestamp"
    end

    test "builds ChartRule struct from empty path" do
      result = ChartRule.build_from_path("")
      assert %ChartRule{} = result
      assert result.path == "timestamp"
    end

    test "builds ChartRule struct from nil path" do
      result = ChartRule.build_from_path(nil)
      assert %ChartRule{} = result
      assert result.path == nil
    end
  end

  describe "build/1" do
    test "builds ChartRule struct from keyword list" do
      params = [
        path: "metadata.latency",
        aggregate: :avg,
        period: :hour,
        value_type: :float
      ]

      result = ChartRule.build(params)

      assert %ChartRule{} = result
      assert result.path == "metadata.latency"
      assert result.aggregate == :avg
      assert result.period == :hour
      assert result.value_type == :float
    end

    test "builds ChartRule struct with defaults for missing fields" do
      params = [path: "custom.field"]

      result = ChartRule.build(params)

      assert %ChartRule{} = result
      assert result.path == "custom.field"
      assert result.aggregate == :count
      assert result.period == :minute
      assert result.value_type == nil
    end

    test "returns empty struct for invalid changeset in build" do
      result = ChartRule.build([])

      assert %ChartRule{} = result
      assert result.path == "timestamp"
      assert result.aggregate == :count
      assert result.period == :minute
    end
  end

  describe "changeset/2" do
    test "creates changeset from existing ChartRule struct" do
      chart_rule = %ChartRule{
        path: "metadata.latency",
        aggregate: :avg,
        period: :hour,
        value_type: :float
      }

      changeset = ChartRule.changeset(%ChartRule{}, chart_rule)

      assert %Ecto.Changeset{} = changeset
      assert changeset.valid?
    end

    test "creates changeset from params map" do
      params = %{
        path: "metadata.requests",
        aggregate: :sum,
        period: :minute,
        value_type: :integer
      }

      changeset = ChartRule.changeset(%ChartRule{}, params)

      assert %Ecto.Changeset{} = changeset
      assert changeset.valid?
    end

    test "creates changeset with string keys" do
      params = %{
        "path" => "timestamp",
        "aggregate" => :count,
        "period" => :hour
      }

      changeset = ChartRule.changeset(%ChartRule{}, params)

      assert %Ecto.Changeset{} = changeset
      assert changeset.valid?
    end
  end

  describe "update/2" do
    test "updates ChartRule with valid parameters" do
      chart_rule = %ChartRule{
        path: "timestamp",
        aggregate: :count,
        period: :minute
      }

      updated =
        ChartRule.update(chart_rule, %{
          aggregate: :avg,
          period: :hour,
          path: "metadata.latency"
        })

      assert %ChartRule{} = updated
      assert updated.aggregate == :avg
      assert updated.period == :hour
      assert updated.path == "metadata.latency"
    end

    test "returns original ChartRule when update parameters are invalid" do
      chart_rule = %ChartRule{
        path: "timestamp",
        aggregate: :count,
        period: :minute
      }

      # Test with invalid parameter types
      updated =
        ChartRule.update(chart_rule, %{
          aggregate: "invalid_atom",
          period: 123
        })

      # Should return original unchanged
      assert updated == chart_rule
    end

    test "partially updates ChartRule with mixed valid/invalid parameters" do
      chart_rule = %ChartRule{
        path: "timestamp",
        aggregate: :count,
        period: :minute
      }

      # Valid path, invalid aggregate - should return original
      updated =
        ChartRule.update(chart_rule, %{
          path: "metadata.requests",
          aggregate: "not_an_atom"
        })

      assert updated == chart_rule
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
