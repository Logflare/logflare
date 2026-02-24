defmodule Logflare.Lql.RulesTest do
  use ExUnit.Case

  alias Logflare.Lql.Rules
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.FromRule
  alias Logflare.Lql.Rules.SelectRule

  describe "get_filter_rules/1" do
    test "extracts only FilterRule structs from mixed list" do
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}
      select_rule = %SelectRule{path: "field", wildcard: false}

      lql_rules = [chart_rule, filter_rule, select_rule]

      result = Rules.get_filter_rules(lql_rules)

      assert result == [filter_rule]
    end

    test "returns empty list when no FilterRules present" do
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}
      select_rule = %SelectRule{path: "field", wildcard: false}

      lql_rules = [chart_rule, select_rule]

      result = Rules.get_filter_rules(lql_rules)

      assert result == []
    end
  end

  describe "get_chart_rules/1" do
    test "extracts only ChartRule structs from mixed list" do
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}
      select_rule = %SelectRule{path: "field", wildcard: false}

      lql_rules = [chart_rule, filter_rule, select_rule]

      result = Rules.get_chart_rules(lql_rules)

      assert result == [chart_rule]
    end

    test "returns empty list when no ChartRules present" do
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}
      select_rule = %SelectRule{path: "field", wildcard: false}

      lql_rules = [filter_rule, select_rule]

      result = Rules.get_chart_rules(lql_rules)

      assert result == []
    end
  end

  describe "get_select_rules/1" do
    test "extracts only SelectRule structs from mixed list" do
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}
      select_rule = %SelectRule{path: "field", wildcard: false}

      lql_rules = [chart_rule, filter_rule, select_rule]

      result = Rules.get_select_rules(lql_rules)

      assert result == [select_rule]
    end

    test "returns empty list when no SelectRules present" do
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}

      lql_rules = [chart_rule, filter_rule]

      result = Rules.get_select_rules(lql_rules)

      assert result == []
    end
  end

  describe "get_chart_rule/1" do
    test "returns first ChartRule when present" do
      chart_rule1 = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}
      chart_rule2 = %ChartRule{aggregate: :avg, path: "latency", period: :hour}
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}

      lql_rules = [filter_rule, chart_rule1, chart_rule2]

      result = Rules.get_chart_rule(lql_rules)

      assert result == chart_rule1
    end

    test "returns nil when no ChartRule present" do
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}
      select_rule = %SelectRule{path: "field", wildcard: false}

      lql_rules = [filter_rule, select_rule]

      result = Rules.get_chart_rule(lql_rules)

      assert result == nil
    end
  end

  describe "normalize_all_rules/1" do
    test "normalizes SelectRules while preserving other rules" do
      select_rule1 = %SelectRule{path: "field1", wildcard: false}
      select_rule2 = %SelectRule{path: "*", wildcard: true}
      select_rule3 = %SelectRule{path: "field2", wildcard: false}
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}

      lql_rules = [select_rule1, chart_rule, select_rule2, filter_rule, select_rule3]

      result = Rules.normalize_all_rules(lql_rules)

      # Should apply wildcard precedence (only wildcard rule remains)
      select_rules = Rules.get_select_rules(result)
      assert length(select_rules) == 1
      assert hd(select_rules).wildcard == true
      assert hd(select_rules).path == "*"

      # Other rules should remain unchanged
      assert Rules.get_chart_rules(result) == [chart_rule]
      assert Rules.get_filter_rules(result) == [filter_rule]
    end
  end

  describe "get_selected_fields/1" do
    test "extracts field paths from SelectRules" do
      select_rule1 = %SelectRule{path: "field1", wildcard: false}
      select_rule2 = %SelectRule{path: "field2", wildcard: false}
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}

      lql_rules = [select_rule1, chart_rule, select_rule2]

      result = Rules.get_selected_fields(lql_rules)

      assert result == ["field1", "field2"]
    end

    test "rejects nil paths" do
      select_rule1 = %SelectRule{path: "field1", wildcard: false}
      select_rule2 = %SelectRule{path: nil, wildcard: false}

      lql_rules = [select_rule1, select_rule2]

      result = Rules.get_selected_fields(lql_rules)

      assert result == ["field1"]
    end
  end

  describe "has_wildcard_selection?/1" do
    test "returns true when wildcard SelectRule present" do
      select_rule1 = %SelectRule{path: "field1", wildcard: false}
      select_rule2 = %SelectRule{path: "*", wildcard: true}

      lql_rules = [select_rule1, select_rule2]

      result = Rules.has_wildcard_selection?(lql_rules)

      assert result == true
    end

    test "returns false when no wildcard SelectRule present" do
      select_rule1 = %SelectRule{path: "field1", wildcard: false}
      select_rule2 = %SelectRule{path: "field2", wildcard: false}

      lql_rules = [select_rule1, select_rule2]

      result = Rules.has_wildcard_selection?(lql_rules)

      assert result == false
    end

    test "returns false when no SelectRules present" do
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}

      lql_rules = [chart_rule]

      result = Rules.has_wildcard_selection?(lql_rules)

      assert result == false
    end
  end

  describe "get_chart_period/2" do
    test "returns period from first ChartRule" do
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :hour}
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}

      lql_rules = [filter_rule, chart_rule]

      result = Rules.get_chart_period(lql_rules)

      assert result == :hour
    end

    test "returns default when no ChartRule present" do
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}

      lql_rules = [filter_rule]

      result = Rules.get_chart_period(lql_rules, :default_period)

      assert result == :default_period
    end

    test "returns nil as default when no ChartRule present and no default given" do
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}

      lql_rules = [filter_rule]

      result = Rules.get_chart_period(lql_rules)

      assert result == nil
    end
  end

  describe "get_chart_aggregate/2" do
    test "returns aggregate from first ChartRule" do
      chart_rule = %ChartRule{aggregate: :avg, path: "latency", period: :minute}
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}

      lql_rules = [filter_rule, chart_rule]

      result = Rules.get_chart_aggregate(lql_rules)

      assert result == :avg
    end

    test "returns default when no ChartRule present" do
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}

      lql_rules = [filter_rule]

      result = Rules.get_chart_aggregate(lql_rules, :default_agg)

      assert result == :default_agg
    end
  end

  describe "put_chart_period/2" do
    test "updates period of existing ChartRule" do
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}

      lql_rules = [filter_rule, chart_rule]

      result = Rules.put_chart_period(lql_rules, :hour)

      updated_chart = Rules.get_chart_rule(result)
      assert updated_chart.period == :hour
      assert updated_chart.aggregate == :count
      assert updated_chart.path == "timestamp"
    end

    test "returns unchanged list when no ChartRule present" do
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}

      lql_rules = [filter_rule]

      result = Rules.put_chart_period(lql_rules, :hour)

      assert result == lql_rules
    end
  end

  describe "update_chart_rule/3" do
    test "updates existing ChartRule with new parameters" do
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}
      default_chart = %ChartRule{aggregate: :avg, path: "latency", period: :hour}

      lql_rules = [filter_rule, chart_rule]

      result = Rules.update_chart_rule(lql_rules, default_chart, %{aggregate: :sum, period: :day})

      updated_chart = Rules.get_chart_rule(result)
      assert updated_chart.aggregate == :sum
      assert updated_chart.period == :day
      assert updated_chart.path == "timestamp"
    end

    test "adds default ChartRule when none exists" do
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}
      default_chart = %ChartRule{aggregate: :avg, path: "latency", period: :hour}

      lql_rules = [filter_rule]

      result = Rules.update_chart_rule(lql_rules, default_chart, %{aggregate: :sum})

      assert length(result) == 2
      chart_rule = Rules.get_chart_rule(result)
      assert chart_rule == default_chart
    end
  end

  describe "put_new_chart_rule/2" do
    test "adds ChartRule when none exists" do
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}

      lql_rules = [filter_rule]

      result = Rules.put_new_chart_rule(lql_rules, chart_rule)

      assert length(result) == 2
      assert Rules.get_chart_rule(result) == chart_rule
    end

    test "does not add ChartRule when one already exists" do
      existing_chart = %ChartRule{aggregate: :avg, path: "latency", period: :hour}
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}
      new_chart = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}

      lql_rules = [filter_rule, existing_chart]

      result = Rules.put_new_chart_rule(lql_rules, new_chart)

      assert result == lql_rules
      assert Rules.get_chart_rule(result) == existing_chart
    end
  end

  describe "upsert_filter_rule_by_path/2" do
    test "replaces existing filter rule at same path and keeps first index" do
      level_filter = %FilterRule{path: "metadata.level", operator: :=, value: "info"}
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}
      message_filter = %FilterRule{path: "event_message", operator: :=, value: "timeout"}
      replacement_filter = %FilterRule{path: "metadata.level", operator: :=, value: "error"}

      lql_rules = [message_filter, level_filter, chart_rule]

      result = Rules.upsert_filter_rule_by_path(lql_rules, replacement_filter)

      assert Enum.at(result, 1) == replacement_filter
      refute Enum.any?(result, &(&1 == level_filter))
      assert message_filter in result
      assert chart_rule in result
    end

    test "appends filter rule when no filter exists for path" do
      message_filter = %FilterRule{path: "event_message", operator: :=, value: "timeout"}
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}
      level_filter = %FilterRule{path: "metadata.level", operator: :=, value: "error"}

      lql_rules = [message_filter, chart_rule]

      result = Rules.upsert_filter_rule_by_path(lql_rules, level_filter)

      assert result == [message_filter, chart_rule, level_filter]
    end

    test "dedupes multiple existing filters with same path" do
      old_filter_1 = %FilterRule{path: "metadata.level", operator: :=, value: "info"}
      message_filter = %FilterRule{path: "event_message", operator: :=, value: "timeout"}
      old_filter_2 = %FilterRule{path: "metadata.level", operator: :=, value: "warn"}
      replacement_filter = %FilterRule{path: "metadata.level", operator: :=, value: "error"}

      lql_rules = [old_filter_1, message_filter, old_filter_2]

      result = Rules.upsert_filter_rule_by_path(lql_rules, replacement_filter)

      assert Enum.at(result, 0) == replacement_filter
      assert message_filter in result

      assert length(Enum.filter(result, &match?(%FilterRule{path: "metadata.level"}, &1))) == 1
    end
  end

  describe "get_from_rule/1" do
    test "returns FromRule when present" do
      from_rule = %FromRule{table: "my_table", table_type: :cte}
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}

      lql_rules = [filter_rule, from_rule, chart_rule]

      result = Rules.get_from_rule(lql_rules)

      assert result == from_rule
    end

    test "returns nil when no FromRule present" do
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}

      lql_rules = [filter_rule, chart_rule]

      result = Rules.get_from_rule(lql_rules)

      assert is_nil(result)
    end
  end

  describe "remove_from_rule/1" do
    test "removes FromRule when present" do
      from_rule = %FromRule{table: "my_table", table_type: :cte}
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}

      lql_rules = [filter_rule, from_rule, chart_rule]

      result = Rules.remove_from_rule(lql_rules)

      assert length(result) == 2
      assert Rules.get_from_rule(result) == nil
      assert filter_rule in result
      assert chart_rule in result
    end

    test "returns unchanged list when no FromRule present" do
      filter_rule = %FilterRule{path: "message", operator: :=, value: "error"}
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}

      lql_rules = [filter_rule, chart_rule]

      result = Rules.remove_from_rule(lql_rules)

      assert result == lql_rules
    end
  end

  describe "default_chart_rule/0" do
    test "returns default ChartRule with expected values" do
      result = Rules.default_chart_rule()

      assert result.aggregate == :count
      assert result.path == "timestamp"
      assert result.period == :minute
      assert result.value_type == :datetime
    end
  end

  describe "default_select_rule/0" do
    test "returns default SelectRule with wildcard" do
      result = Rules.default_select_rule()

      assert result.path == "*"
      assert result.wildcard == true
    end
  end

  describe "get_timestamp_filters/1" do
    test "extracts only timestamp FilterRules" do
      timestamp_filter = %FilterRule{
        path: "timestamp",
        operator: :>,
        value: ~N[2023-01-01 00:00:00]
      }

      message_filter = %FilterRule{path: "message", operator: :=, value: "error"}
      metadata_filter = %FilterRule{path: "metadata.level", operator: :=, value: "info"}
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}

      lql_rules = [timestamp_filter, chart_rule, message_filter, metadata_filter]

      result = Rules.get_timestamp_filters(lql_rules)

      assert result == [timestamp_filter]
    end

    test "returns empty list when no timestamp filters present" do
      message_filter = %FilterRule{path: "message", operator: :=, value: "error"}
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}

      lql_rules = [message_filter, chart_rule]

      result = Rules.get_timestamp_filters(lql_rules)

      assert result == []
    end
  end

  describe "get_metadata_and_message_filters/1" do
    test "extracts non-timestamp FilterRules" do
      timestamp_filter = %FilterRule{
        path: "timestamp",
        operator: :>,
        value: ~N[2023-01-01 00:00:00]
      }

      message_filter = %FilterRule{path: "message", operator: :=, value: "error"}
      metadata_filter = %FilterRule{path: "metadata.level", operator: :=, value: "info"}
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}

      lql_rules = [timestamp_filter, chart_rule, message_filter, metadata_filter]

      result = Rules.get_metadata_and_message_filters(lql_rules)

      assert result == [message_filter, metadata_filter]
    end

    test "returns empty list when only timestamp filters present" do
      timestamp_filter = %FilterRule{
        path: "timestamp",
        operator: :>,
        value: ~N[2023-01-01 00:00:00]
      }

      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}

      lql_rules = [timestamp_filter, chart_rule]

      result = Rules.get_metadata_and_message_filters(lql_rules)

      assert result == []
    end
  end

  describe "update_timestamp_rules/2" do
    test "replaces all timestamp filters with new ones" do
      old_timestamp1 = %FilterRule{
        path: "timestamp",
        operator: :>,
        value: ~N[2023-01-01 00:00:00]
      }

      old_timestamp2 = %FilterRule{
        path: "timestamp",
        operator: :<,
        value: ~N[2023-12-31 23:59:59]
      }

      message_filter = %FilterRule{path: "message", operator: :=, value: "error"}
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}

      new_timestamp = %FilterRule{
        path: "timestamp",
        operator: :range,
        values: [~N[2024-01-01 00:00:00], ~N[2024-01-31 23:59:59]]
      }

      lql_rules = [old_timestamp1, message_filter, chart_rule, old_timestamp2]

      result = Rules.update_timestamp_rules(lql_rules, [new_timestamp])

      timestamp_filters = Rules.get_timestamp_filters(result)
      assert timestamp_filters == [new_timestamp]

      # Other rules should remain
      assert Rules.get_metadata_and_message_filters(result) == [message_filter]
      assert Rules.get_chart_rule(result) == chart_rule
    end

    test "adds new timestamp filters when none existed" do
      message_filter = %FilterRule{path: "message", operator: :=, value: "error"}
      chart_rule = %ChartRule{aggregate: :count, path: "timestamp", period: :minute}

      new_timestamp = %FilterRule{path: "timestamp", operator: :>, value: ~N[2024-01-01 00:00:00]}

      lql_rules = [message_filter, chart_rule]

      result = Rules.update_timestamp_rules(lql_rules, [new_timestamp])

      timestamp_filters = Rules.get_timestamp_filters(result)
      assert timestamp_filters == [new_timestamp]
    end
  end

  describe "jump_timestamp/2" do
    test "creates new timestamp range by jumping forwards" do
      timestamp_filter = %FilterRule{
        path: "timestamp",
        operator: :range,
        values: [~N[2023-01-01 10:00:00], ~N[2023-01-01 11:00:00]]
      }

      message_filter = %FilterRule{path: "message", operator: :=, value: "error"}

      lql_rules = [timestamp_filter, message_filter]

      result = Rules.jump_timestamp(lql_rules, :forwards)

      new_timestamp_filters = Rules.get_timestamp_filters(result)
      assert length(new_timestamp_filters) == 1

      new_filter = hd(new_timestamp_filters)
      assert new_filter.path == "timestamp"
      assert new_filter.operator == :range
      assert length(new_filter.values) == 2

      # Should jump forward by 1 hour (the original range duration)
      [new_from, new_to] = new_filter.values
      assert new_from == ~N[2023-01-01 11:00:00.000000]
      assert new_to == ~N[2023-01-01 12:00:00.000000]
    end

    test "creates new timestamp range by jumping backwards" do
      timestamp_filter = %FilterRule{
        path: "timestamp",
        operator: :range,
        values: [~N[2023-01-01 10:00:00], ~N[2023-01-01 11:00:00]]
      }

      message_filter = %FilterRule{path: "message", operator: :=, value: "error"}

      lql_rules = [timestamp_filter, message_filter]

      result = Rules.jump_timestamp(lql_rules, :backwards)

      new_timestamp_filters = Rules.get_timestamp_filters(result)
      assert length(new_timestamp_filters) == 1

      new_filter = hd(new_timestamp_filters)
      assert new_filter.path == "timestamp"
      assert new_filter.operator == :range

      # Should jump backward by 1 hour (the original range duration)
      [new_from, new_to] = new_filter.values
      assert new_from == ~N[2023-01-01 09:00:00.000000]
      assert new_to == ~N[2023-01-01 10:00:00.000000]
    end
  end

  describe "timestamp_filter_rule_is_shorthand?/1" do
    test "delegates to FilterRule.is_shorthand_timestamp?" do
      shorthand_filter = %FilterRule{path: "timestamp", shorthand: "today"}
      regular_filter = %FilterRule{path: "timestamp", shorthand: nil}

      assert Rules.timestamp_filter_rule_is_shorthand?(shorthand_filter) == true
      assert Rules.timestamp_filter_rule_is_shorthand?(regular_filter) == false
    end
  end

  describe "get_lql_parser_warnings/2" do
    test "returns warning for routing dialect with timestamp filters" do
      timestamp_filter = %FilterRule{
        path: "timestamp",
        operator: :>,
        value: ~N[2023-01-01 00:00:00]
      }

      message_filter = %FilterRule{path: "message", operator: :=, value: "error"}

      lql_rules = [timestamp_filter, message_filter]

      result = Rules.get_lql_parser_warnings(lql_rules, dialect: :routing)

      assert result == "Timestamp LQL clauses are ignored for event routing"
    end

    test "returns nil for routing dialect without timestamp filters" do
      message_filter = %FilterRule{path: "message", operator: :=, value: "error"}
      metadata_filter = %FilterRule{path: "metadata.level", operator: :=, value: "info"}

      lql_rules = [message_filter, metadata_filter]

      result = Rules.get_lql_parser_warnings(lql_rules, dialect: :routing)

      assert result == nil
    end
  end
end
