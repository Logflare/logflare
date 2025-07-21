defmodule Logflare.Lql.UtilsTest do
  use Logflare.DataCase, async: true

  alias Logflare.Lql.ChartRule
  alias Logflare.Lql.FilterRule
  alias Logflare.Lql.Utils

  describe "default_chart_rule/0" do
    test "returns default chart rule" do
      chart_rule = Utils.default_chart_rule()

      assert %ChartRule{} = chart_rule
      assert chart_rule.aggregate == :count
      assert chart_rule.path == "timestamp"
      assert chart_rule.period == :minute
      assert chart_rule.value_type == :datetime
    end
  end

  describe "get_chart_period/2" do
    test "returns chart period from rules with chart" do
      rules = [
        %FilterRule{path: "event_message", value: "test"},
        %ChartRule{period: :hour}
      ]

      result = Utils.get_chart_period(rules)
      assert result == :hour
    end

    test "returns default when no chart rule present" do
      rules = [
        %FilterRule{path: "event_message", value: "test"}
      ]

      result = Utils.get_chart_period(rules, :default_period)
      assert result == :default_period
    end

    test "returns nil default when no chart rule and no default provided" do
      rules = [
        %FilterRule{path: "event_message", value: "test"}
      ]

      result = Utils.get_chart_period(rules)
      assert result == nil
    end

    test "returns period from first chart rule when multiple present" do
      rules = [
        %ChartRule{period: :minute},
        %ChartRule{period: :hour}
      ]

      result = Utils.get_chart_period(rules)
      assert result == :minute
    end
  end

  describe "get_chart_aggregate/2" do
    test "returns chart aggregate from rules with chart" do
      rules = [
        %FilterRule{path: "event_message", value: "test"},
        %ChartRule{aggregate: :sum}
      ]

      result = Utils.get_chart_aggregate(rules)
      assert result == :sum
    end

    test "returns default when no chart rule present" do
      rules = [
        %FilterRule{path: "event_message", value: "test"}
      ]

      result = Utils.get_chart_aggregate(rules, :default_agg)
      assert result == :default_agg
    end

    test "returns nil default when no chart rule and no default provided" do
      rules = [
        %FilterRule{path: "event_message", value: "test"}
      ]

      result = Utils.get_chart_aggregate(rules)
      assert result == nil
    end
  end

  describe "get_filter_rules/1" do
    test "filters out only filter rules" do
      rules = [
        %FilterRule{path: "event_message", value: "test"},
        %ChartRule{aggregate: :count},
        %FilterRule{path: "metadata.level", value: "info"}
      ]

      result = Utils.get_filter_rules(rules)

      assert length(result) == 2
      assert Enum.all?(result, &match?(%FilterRule{}, &1))
    end

    test "returns empty list when no filter rules" do
      rules = [
        %ChartRule{aggregate: :count},
        %ChartRule{aggregate: :sum}
      ]

      result = Utils.get_filter_rules(rules)
      assert result == []
    end

    test "returns empty list for empty input" do
      result = Utils.get_filter_rules([])
      assert result == []
    end
  end

  describe "get_chart_rules/1" do
    test "filters out only chart rules" do
      rules = [
        %FilterRule{path: "event_message", value: "test"},
        %ChartRule{aggregate: :count},
        %FilterRule{path: "metadata.level", value: "info"},
        %ChartRule{aggregate: :sum}
      ]

      result = Utils.get_chart_rules(rules)

      assert length(result) == 2
      assert Enum.all?(result, &match?(%ChartRule{}, &1))
    end

    test "returns empty list when no chart rules" do
      rules = [
        %FilterRule{path: "event_message", value: "test"},
        %FilterRule{path: "metadata.level", value: "info"}
      ]

      result = Utils.get_chart_rules(rules)
      assert result == []
    end
  end

  describe "get_chart_rule/1" do
    test "returns first chart rule found" do
      rules = [
        %FilterRule{path: "event_message", value: "test"},
        %ChartRule{aggregate: :count, period: :hour},
        %ChartRule{aggregate: :sum, period: :minute}
      ]

      result = Utils.get_chart_rule(rules)

      assert %ChartRule{} = result
      assert result.aggregate == :count
      assert result.period == :hour
    end

    test "returns nil when no chart rule present" do
      rules = [
        %FilterRule{path: "event_message", value: "test"}
      ]

      result = Utils.get_chart_rule(rules)
      assert result == nil
    end
  end

  describe "update_timestamp_rules/2" do
    test "replaces existing timestamp rules with new ones" do
      rules = [
        %FilterRule{path: "timestamp", operator: :>, value: ~N[2020-01-01 00:00:00]},
        %FilterRule{path: "event_message", value: "test"},
        %FilterRule{path: "timestamp", operator: :<, value: ~N[2020-01-02 00:00:00]}
      ]

      new_timestamp_rules = [
        %FilterRule{
          path: "timestamp",
          operator: :range,
          values: [~N[2020-01-03 00:00:00], ~N[2020-01-04 00:00:00]]
        }
      ]

      result = Utils.update_timestamp_rules(rules, new_timestamp_rules)

      timestamp_rules = Enum.filter(result, &(&1.path == "timestamp"))
      assert length(timestamp_rules) == 1
      assert hd(timestamp_rules).operator == :range

      non_timestamp_rules = Enum.filter(result, &(&1.path != "timestamp"))
      assert length(non_timestamp_rules) == 1
      assert hd(non_timestamp_rules).path == "event_message"
    end

    test "adds new timestamp rules when none exist" do
      rules = [
        %FilterRule{path: "event_message", value: "test"},
        %FilterRule{path: "metadata.level", value: "info"}
      ]

      new_timestamp_rules = [
        %FilterRule{path: "timestamp", operator: :>, value: ~N[2020-01-01 00:00:00]}
      ]

      result = Utils.update_timestamp_rules(rules, new_timestamp_rules)

      timestamp_rules = Enum.filter(result, &(&1.path == "timestamp"))
      assert length(timestamp_rules) == 1

      assert length(result) == 3
    end
  end

  describe "put_chart_period/2" do
    test "updates chart period in existing chart rule" do
      rules = [
        %FilterRule{path: "event_message", value: "test"},
        %ChartRule{aggregate: :count, period: :minute}
      ]

      result = Utils.put_chart_period(rules, :hour)

      chart_rule = Utils.get_chart_rule(result)
      assert chart_rule.period == :hour
      assert chart_rule.aggregate == :count
    end
  end

  describe "update_chart_rule/3" do
    test "updates existing chart rule with new params" do
      rules = [
        %FilterRule{path: "event_message", value: "test"},
        %ChartRule{aggregate: :count, period: :minute}
      ]

      default_chart = Utils.default_chart_rule()
      params = %{aggregate: :sum, period: :hour}

      result = Utils.update_chart_rule(rules, default_chart, params)

      chart_rule = Utils.get_chart_rule(result)
      assert chart_rule.aggregate == :sum
      assert chart_rule.period == :hour
    end

    test "adds default chart rule when none exists" do
      rules = [
        %FilterRule{path: "event_message", value: "test"}
      ]

      default_chart = Utils.default_chart_rule()
      params = %{aggregate: :sum}

      result = Utils.update_chart_rule(rules, default_chart, params)

      chart_rule = Utils.get_chart_rule(result)
      assert chart_rule == default_chart
      assert length(result) == 2
    end
  end

  describe "put_new_chart_rule/2" do
    test "does not add chart rule when one already exists" do
      existing_chart = %ChartRule{aggregate: :count}

      rules = [
        %FilterRule{path: "event_message", value: "test"},
        existing_chart
      ]

      new_chart = %ChartRule{aggregate: :sum}
      result = Utils.put_new_chart_rule(rules, new_chart)

      assert result == rules
      chart_rule = Utils.get_chart_rule(result)
      assert chart_rule.aggregate == :count
    end

    test "adds chart rule when none exists" do
      rules = [
        %FilterRule{path: "event_message", value: "test"}
      ]

      new_chart = %ChartRule{aggregate: :sum}
      result = Utils.put_new_chart_rule(rules, new_chart)

      assert length(result) == 2
      chart_rule = Utils.get_chart_rule(result)
      assert chart_rule.aggregate == :sum
    end
  end

  describe "get_ts_filters/1" do
    test "returns only timestamp filter rules" do
      rules = [
        %FilterRule{path: "timestamp", operator: :>, value: ~N[2020-01-01 00:00:00]},
        %FilterRule{path: "event_message", value: "test"},
        %FilterRule{
          path: "timestamp",
          operator: :range,
          values: [~N[2020-01-01 00:00:00], ~N[2020-01-02 00:00:00]]
        },
        %ChartRule{aggregate: :count}
      ]

      result = Utils.get_ts_filters(rules)

      timestamp_filter_rules = Enum.filter(result, &match?(%FilterRule{}, &1))
      assert length(timestamp_filter_rules) == 2
      assert Enum.all?(timestamp_filter_rules, &(&1.path == "timestamp"))
    end

    test "returns empty list when no timestamp filters" do
      rules = [
        %FilterRule{path: "event_message", value: "test"},
        %FilterRule{path: "metadata.level", value: "info"}
      ]

      result = Utils.get_ts_filters(rules)
      assert result == []
    end
  end

  describe "get_meta_and_msg_filters/1" do
    test "returns non-timestamp filter rules" do
      rules = [
        %FilterRule{path: "timestamp", operator: :>, value: ~N[2020-01-01 00:00:00]},
        %FilterRule{path: "event_message", value: "test"},
        %FilterRule{path: "metadata.level", value: "info"},
        %ChartRule{aggregate: :count}
      ]

      result = Utils.get_meta_and_msg_filters(rules)

      assert length(result) == 2
      assert Enum.all?(result, &match?(%FilterRule{}, &1))
      assert Enum.all?(result, &(&1.path != "timestamp"))
      paths = Enum.map(result, & &1.path)
      assert "event_message" in paths
      assert "metadata.level" in paths
    end

    test "returns empty list when only timestamp filters" do
      rules = [
        %FilterRule{path: "timestamp", operator: :>, value: ~N[2020-01-01 00:00:00]}
      ]

      result = Utils.get_meta_and_msg_filters(rules)
      assert result == []
    end
  end

  describe "get_lql_parser_warnings/2" do
    test "returns warning when timestamp filters present for routing dialect" do
      rules = [
        %FilterRule{path: "timestamp", operator: :>, value: ~N[2020-01-01 00:00:00]},
        %FilterRule{path: "event_message", value: "test"}
      ]

      result = Utils.get_lql_parser_warnings(rules, dialect: :routing)
      assert result == "Timestamp LQL clauses are ignored for event routing"
    end

    test "returns nil when no timestamp filters for routing dialect" do
      rules = [
        %FilterRule{path: "event_message", value: "test"},
        %FilterRule{path: "metadata.level", value: "info"}
      ]

      result = Utils.get_lql_parser_warnings(rules, dialect: :routing)
      assert result == nil
    end
  end

  describe "jump_timestamp/2" do
    test "jumps timestamp forwards" do
      rules = [
        %FilterRule{
          path: "timestamp",
          operator: :range,
          values: [~N[2020-01-01 10:00:00], ~N[2020-01-01 11:00:00]]
        },
        %FilterRule{path: "event_message", value: "test"}
      ]

      result = Utils.jump_timestamp(rules, :forwards)

      timestamp_rules = Utils.get_ts_filters(result)
      assert length(timestamp_rules) == 1

      rule = hd(timestamp_rules)
      assert rule.operator == :range
      [from, to] = rule.values

      assert NaiveDateTime.compare(from, ~N[2020-01-01 11:00:00]) == :eq
      assert NaiveDateTime.compare(to, ~N[2020-01-01 12:00:00]) == :eq
    end

    test "jumps timestamp backwards" do
      rules = [
        %FilterRule{
          path: "timestamp",
          operator: :range,
          values: [~N[2020-01-01 10:00:00], ~N[2020-01-01 11:00:00]]
        },
        %FilterRule{path: "event_message", value: "test"}
      ]

      result = Utils.jump_timestamp(rules, :backwards)

      timestamp_rules = Utils.get_ts_filters(result)
      assert length(timestamp_rules) == 1

      rule = hd(timestamp_rules)
      assert rule.operator == :range
      [from, to] = rule.values

      assert NaiveDateTime.compare(from, ~N[2020-01-01 09:00:00]) == :eq
      assert NaiveDateTime.compare(to, ~N[2020-01-01 10:00:00]) == :eq
    end

    test "preserves non-timestamp rules when jumping" do
      rules = [
        %FilterRule{
          path: "timestamp",
          operator: :range,
          values: [~N[2020-01-01 10:00:00], ~N[2020-01-01 11:00:00]]
        },
        %FilterRule{path: "event_message", value: "test"},
        %FilterRule{path: "metadata.level", value: "info"}
      ]

      result = Utils.jump_timestamp(rules, :forwards)

      non_timestamp_rules = Utils.get_meta_and_msg_filters(result)
      assert length(non_timestamp_rules) == 2

      paths = Enum.map(non_timestamp_rules, & &1.path)
      assert "event_message" in paths
      assert "metadata.level" in paths
    end
  end

  describe "timestamp_filter_rule_is_shorthand?/1" do
    test "returns true for 'last@' shorthand" do
      rule = %FilterRule{shorthand: "last@5minute"}
      assert Utils.timestamp_filter_rule_is_shorthand?(rule) == true
    end

    test "returns true for 'this@' shorthand" do
      rule = %FilterRule{shorthand: "this@hour"}
      assert Utils.timestamp_filter_rule_is_shorthand?(rule) == true
    end

    test "returns true for 'today' shorthand" do
      rule = %FilterRule{shorthand: "today"}
      assert Utils.timestamp_filter_rule_is_shorthand?(rule) == true
    end

    test "returns true for 'yesterday' shorthand" do
      rule = %FilterRule{shorthand: "yesterday"}
      assert Utils.timestamp_filter_rule_is_shorthand?(rule) == true
    end

    test "returns false for 'now' shorthand" do
      rule = %FilterRule{shorthand: "now"}
      assert Utils.timestamp_filter_rule_is_shorthand?(rule) == false
    end

    test "returns false for nil shorthand" do
      rule = %FilterRule{shorthand: nil}
      assert Utils.timestamp_filter_rule_is_shorthand?(rule) == false
    end

    test "returns false for regular string shorthand" do
      rule = %FilterRule{shorthand: "custom"}
      assert Utils.timestamp_filter_rule_is_shorthand?(rule) == false
    end
  end
end
