defmodule Logflare.Lql.Parser.HelpersTest do
  use ExUnit.Case, async: true
  use Mimic

  alias Logflare.Lql.Parser.Helpers
  alias Logflare.Lql.Rules.FilterRule

  describe "to_rule/2 functions" do
    test "handles quoted field value" do
      args = [isolated_string: "test value"]
      result = Helpers.to_rule(args, :quoted_field_value)
      assert result == {:quoted, "test value"}
    end

    test "handles quoted event message with operator" do
      args = [isolated_string: "test message", operator: :"~"]
      result = Helpers.to_rule(args, :quoted_event_message)

      assert result.path == "event_message"
      assert result.value == "test message"
      assert result.operator == :"~"
      assert result.modifiers.quoted_string == true
    end

    test "handles quoted event message without operator" do
      args = [isolated_string: "test message"]
      result = Helpers.to_rule(args, :quoted_event_message)

      assert result.path == "event_message"
      assert result.value == "test message"
      assert result.operator == :string_contains
      assert result.modifiers.quoted_string == true
    end

    test "handles event message with operator" do
      args = [word: "test", operator: :"~"]
      result = Helpers.to_rule(args, :event_message)

      assert result.path == "event_message"
      assert result.value == "test"
      assert result.operator == :"~"
    end

    test "handles event message without operator" do
      args = [word: "test"]
      result = Helpers.to_rule(args, :event_message)

      assert result.path == "event_message"
      assert result.value == "test"
      assert result.operator == :string_contains
    end

    test "handles filter with quoted value" do
      args = [path: "field1", operator: :=, value: {:quoted, "test value"}]
      result = Helpers.to_rule(args, :filter)

      assert result.path == "field1"
      assert result.value == "test value"
      assert result.operator == :=
      assert result.modifiers.quoted_string == true
    end

    test "handles filter with datetime_with_range single value" do
      args = [
        path: "timestamp",
        operator: :=,
        value: {:datetime_with_range, [[~N[2023-01-01 12:00:00]]]}
      ]

      result = Helpers.to_rule(args, :filter)

      assert result.path == "timestamp"
      assert result.value == ~N[2023-01-01 12:00:00]
      assert result.operator == :=
    end

    test "handles filter with datetime_with_range range values" do
      args = [
        path: "timestamp",
        operator: :=,
        value: {:datetime_with_range, [[~N[2023-01-01 10:00:00], ~N[2023-01-01 11:00:00]]]}
      ]

      result = Helpers.to_rule(args, :filter)

      assert result.path == "timestamp"
      assert result.value == nil
      assert result.values == [~N[2023-01-01 10:00:00], ~N[2023-01-01 11:00:00]]
      assert result.operator == :range
    end

    test "handles filter_maybe_shorthand with shorthand value" do
      shorthand_value = %{
        shorthand: "this@minutes",
        value: {:range_operator, [~N[2023-01-01 10:30:00], ~N[2023-01-01 10:31:00]]}
      }

      args = [path: "timestamp", operator: :=, value: shorthand_value]
      result = Helpers.to_rule(args, :filter_maybe_shorthand)

      assert result.path == "timestamp"
      assert result.shorthand == "this@minute"
      assert result.value == {:range_operator, [~N[2023-01-01 10:30:00], ~N[2023-01-01 10:31:00]]}
    end

    test "handles filter_maybe_shorthand with plural shorthand trimming" do
      shorthand_value = %{
        shorthand: "last@5minutes",
        value: {:range_operator, [~N[2023-01-01 10:25:00], ~N[2023-01-01 10:30:00]]}
      }

      args = [path: "timestamp", operator: :=, value: shorthand_value]
      result = Helpers.to_rule(args, :filter_maybe_shorthand)

      assert result.shorthand == "last@5minute"
    end

    test "handles filter_maybe_shorthand without shorthand" do
      args = [path: "timestamp", operator: :=, value: ~N[2023-01-01 12:00:00]]
      result = Helpers.to_rule(args, :filter_maybe_shorthand)

      assert result.path == "timestamp"
      assert result.value == ~N[2023-01-01 12:00:00]
      assert is_nil(result.shorthand)
    end
  end

  describe "to_rule/1 for metadata_level_clause" do
    test "converts metadata level range to filter rules" do
      input = [metadata_level_clause: ["metadata.level", {:range_operator, [1, 4]}]]
      result = Helpers.to_rule(input)

      assert is_list(result)
      assert length(result) == 4

      expected_levels = ["info", "notice", "warning", "error"]
      actual_levels = Enum.map(result, & &1.value)

      assert actual_levels == expected_levels

      Enum.each(result, fn rule ->
        assert rule.path == "metadata.level"
        assert rule.operator == :=
        assert rule.modifiers == %{}
      end)
    end

    test "converts single level range" do
      input = [metadata_level_clause: ["metadata.level", {:range_operator, [2, 2]}]]
      result = Helpers.to_rule(input)

      assert length(result) == 1
      assert Enum.at(result, 0).value == "notice"
    end
  end

  describe "apply_value_modifiers/1" do
    test "handles range_operator modifier" do
      rule = %{
        path: "timestamp",
        value: {:range_operator, [~N[2023-01-01 10:00:00], ~N[2023-01-01 11:00:00]]},
        values: nil,
        operator: :=
      }

      result = Helpers.apply_value_modifiers([rule])

      assert result.operator == :range
      assert result.values == [~N[2023-01-01 10:00:00], ~N[2023-01-01 11:00:00]]
      assert result.value == nil
    end

    test "handles datetime_with_range values" do
      rule = %{
        path: "timestamp",
        value:
          {:range_operator,
           [
             {:datetime_with_range, [[~N[2023-01-01 10:00:00]]]},
             {:datetime_with_range, [[~N[2023-01-01 11:00:00]]]}
           ]},
        values: nil,
        operator: :=
      }

      result = Helpers.apply_value_modifiers([rule])

      assert result.operator == :range
      assert result.values == [~N[2023-01-01 10:00:00], ~N[2023-01-01 11:00:00]]
      assert result.value == nil
    end

    test "leaves non-range values unchanged" do
      rule = %{
        path: "event_message",
        value: "test message",
        values: nil,
        operator: :=
      }

      result = Helpers.apply_value_modifiers([rule])

      assert result.value == "test message"
      assert result.operator == :=
    end
  end

  describe "maybe_apply_negation_modifier/1" do
    test "applies negation to list of rules" do
      rules = [
        %FilterRule{path: "field1", value: "value1", modifiers: %{}},
        %FilterRule{path: "field2", value: "value2", modifiers: %{}}
      ]

      result = Helpers.maybe_apply_negation_modifier([:negate, rules])

      assert length(result) == 2
      assert Enum.all?(result, fn rule -> rule.modifiers.negate == true end)
      assert Enum.at(result, 0).path == "field1"
      assert Enum.at(result, 1).path == "field2"
    end

    test "applies negation to single rule" do
      rule = %FilterRule{path: "field", value: "value", modifiers: %{}}

      result = Helpers.maybe_apply_negation_modifier([:negate, rule])

      assert result.modifiers.negate == true
      assert result.path == "field"
    end

    test "passes through rule without negation" do
      rule = %FilterRule{path: "field", value: "value", modifiers: %{}}

      result = Helpers.maybe_apply_negation_modifier(rule)

      assert result == rule
      assert Map.get(result.modifiers, :negate) == nil
    end
  end

  describe "get_level_order/1" do
    test "returns correct order for valid levels" do
      assert Helpers.get_level_order("debug") == 0
      assert Helpers.get_level_order("info") == 1
      assert Helpers.get_level_order("notice") == 2
      assert Helpers.get_level_order("warning") == 3
      assert Helpers.get_level_order("error") == 4
      assert Helpers.get_level_order("critical") == 5
      assert Helpers.get_level_order("alert") == 6
      assert Helpers.get_level_order("emergency") == 7
    end

    test "returns nil for invalid level" do
      assert Helpers.get_level_order("invalid") == nil
      assert Helpers.get_level_order("") == nil
    end
  end

  describe "datetime helper functions" do
    test "parse_date_or_datetime handles date" do
      result = Helpers.parse_date_or_datetime([{:date, "2023-01-15"}])
      assert result == ~D[2023-01-15]
    end

    test "parse_date_or_datetime handles datetime" do
      result = Helpers.parse_date_or_datetime([{:datetime, "2023-01-15T10:30:00"}])
      assert result == ~N[2023-01-15 10:30:00]
    end

    test "timestamp_shorthand_to_value handles 'now'" do
      result = Helpers.timestamp_shorthand_to_value(["now"])
      assert result.shorthand == "now"
      assert %DateTime{} = result.value
    end

    test "timestamp_shorthand_to_value handles 'today'" do
      result = Helpers.timestamp_shorthand_to_value(["today"])
      assert result.shorthand == "today"
      assert match?({:range_operator, [%DateTime{}, %DateTime{}]}, result.value)
    end

    test "timestamp_shorthand_to_value handles 'yesterday'" do
      result = Helpers.timestamp_shorthand_to_value(["yesterday"])
      assert result.shorthand == "yesterday"
      assert match?({:range_operator, [%DateTime{}, %DateTime{}]}, result.value)
    end

    test "timestamp_shorthand_to_value handles 'this@period'" do
      result = Helpers.timestamp_shorthand_to_value(["this", :hours])
      assert result.shorthand == "this@hours"
      assert match?({:range_operator, [%DateTime{}, %DateTime{}]}, result.value)
    end

    test "timestamp_shorthand_to_value handles 'last@amount period'" do
      result = Helpers.timestamp_shorthand_to_value(["last", 5, :minutes])
      assert result.shorthand == "last@5minutes"
      assert match?({:range_operator, [%DateTime{}, %DateTime{}]}, result.value)
    end
  end

  describe "validation functions" do
    test "check_for_no_invalid_metadata_field_values allows valid rules" do
      rule = %{path: "field", value: "valid_value"}
      result = Helpers.check_for_no_invalid_metadata_field_values(rule, :metadata)
      assert result == rule
    end

    test "check_for_no_invalid_metadata_field_values throws for invalid timestamp" do
      rule = %{path: "timestamp", value: {:invalid_metadata_field_value, "bad_value"}}

      assert catch_throw(Helpers.check_for_no_invalid_metadata_field_values(rule, :timestamp)) =~
               "Error while parsing timestamp filter value"
    end

    test "check_for_no_invalid_metadata_field_values throws for invalid metadata" do
      rule = %{path: "metadata.field", value: {:invalid_metadata_field_value, "bad_value"}}

      assert catch_throw(Helpers.check_for_no_invalid_metadata_field_values(rule, :metadata)) =~
               "Error while parsing `metadata.field` field"
    end
  end

  describe "ISO8601 parsing error handling" do
    test "handles ISO8601 parsing with timezone offset" do
      stub(Date, :from_iso8601, fn _ -> {:ok, ~D[2023-01-01], "+00:00"} end)

      result = Helpers.parse_date_or_datetime([{:date, "2023-01-01"}])
      assert result == ~D[2023-01-01]
    end

    test "handles invalid ISO8601 format error" do
      stub(Date, :from_iso8601, fn _ -> {:error, :invalid_format} end)

      error = catch_throw(Helpers.parse_date_or_datetime([{:date, "invalid-date"}]))
      assert error =~ "Error while parsing timestamp date value: expected ISO8601 string, got"
    end

    test "handles generic ISO8601 error" do
      stub(Date, :from_iso8601, fn _ -> {:error, "custom error"} end)

      error = catch_throw(Helpers.parse_date_or_datetime([{:date, "2023-01-01"}]))
      assert error == "custom error"
    end
  end
end
