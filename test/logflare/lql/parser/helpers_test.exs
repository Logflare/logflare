defmodule Logflare.Lql.Parser.HelpersTest do
  use Logflare.DataCase, async: true

  alias Logflare.Lql.FilterRule
  alias Logflare.Lql.Parser.Helpers

  describe "parse_date_or_datetime/1" do
    test "parses valid date" do
      input = [{:date, "2023-12-25"}]
      result = Helpers.parse_date_or_datetime(input)
      assert result == ~D[2023-12-25]
    end

    test "parses valid datetime without timezone" do
      input = [{:datetime, "2023-12-25T14:30:45"}]
      result = Helpers.parse_date_or_datetime(input)
      assert result == ~N[2023-12-25 14:30:45]
    end

    test "parses valid datetime with Z timezone" do
      input = [{:datetime, "2023-12-25T14:30:45Z"}]
      result = Helpers.parse_date_or_datetime(input)
      assert result == ~N[2023-12-25 14:30:45]
    end

    test "parses valid datetime with microseconds" do
      input = [{:datetime, "2023-12-25T14:30:45.123456"}]
      result = Helpers.parse_date_or_datetime(input)
      assert result == ~N[2023-12-25 14:30:45.123456]
    end

    test "parses valid datetime with offset" do
      input = [{:datetime, "2023-12-25T14:30:45+02:00"}]
      result = Helpers.parse_date_or_datetime(input)
      assert result == ~N[2023-12-25 14:30:45]
    end

    test "parses date and returns Date struct (without offset)" do
      input = [{:date, "2023-12-25"}]
      result = Helpers.parse_date_or_datetime(input)
      assert result == ~D[2023-12-25]
      assert Date.to_string(result) == "2023-12-25"
    end

    test "parses datetime without offset and returns NaiveDateTime" do
      input = [{:datetime, "2023-12-25T14:30:45"}]
      result = Helpers.parse_date_or_datetime(input)
      assert result == ~N[2023-12-25 14:30:45]
      assert NaiveDateTime.to_string(result) == "2023-12-25 14:30:45"
    end

    test "throws error for invalid date format" do
      input = [{:date, "invalid-date"}]

      assert catch_throw(Helpers.parse_date_or_datetime(input)) =~
               "Error while parsing timestamp date value: expected ISO8601 string, got 'invalid-date'"
    end

    test "throws error for invalid datetime format" do
      input = [{:datetime, "2023-13-45T25:70:90"}]

      result = catch_throw(Helpers.parse_date_or_datetime(input))
      assert result == :invalid_date
    end

    test "throws specific error for invalid format" do
      input = [{:date, "not-a-date-at-all"}]

      result = catch_throw(Helpers.parse_date_or_datetime(input))

      assert result ==
               "Error while parsing timestamp date value: expected ISO8601 string, got 'not-a-date-at-all'"
    end

    test "handles other parsing errors" do
      input = [{:datetime, "2023-02-30T14:30:45"}]

      result = catch_throw(Helpers.parse_date_or_datetime(input))
      assert result == :invalid_date
    end

    test "handles leap year dates" do
      input = [{:date, "2024-02-29"}]
      result = Helpers.parse_date_or_datetime(input)
      assert result == ~D[2024-02-29]
    end

    test "throws error for invalid leap year" do
      input = [{:date, "2023-02-29"}]

      result = catch_throw(Helpers.parse_date_or_datetime(input))
      assert result == :invalid_date
    end
  end

  describe "parse_date_or_datetime_with_range/1" do
    test "creates date range" do
      input = [year: [2023, 2024], month: [1, 2], day: [1, 15]]
      result = Helpers.parse_date_or_datetime_with_range(input)

      assert result == [~D[2023-01-01], ~D[2024-02-15]]
    end

    test "creates single date when values are the same" do
      input = [year: [2023], month: [12], day: [25]]
      result = Helpers.parse_date_or_datetime_with_range(input)

      assert result == [~D[2023-12-25]]
    end

    test "creates datetime range with times" do
      input = [
        year: [2023],
        month: [12],
        day: [25],
        hour: [10, 14],
        minute: [30, 45],
        second: [15, 30]
      ]

      result = Helpers.parse_date_or_datetime_with_range(input)

      assert result == [~N[2023-12-25 10:30:15], ~N[2023-12-25 14:45:30]]
    end

    test "handles microseconds in datetime range" do
      input = [
        year: [2023],
        month: [12],
        day: [25],
        hour: [10],
        minute: [30],
        second: [15],
        microsecond: ["123456", "789012"]
      ]

      result = Helpers.parse_date_or_datetime_with_range(input)

      assert result == [~N[2023-12-25 10:30:15.123456], ~N[2023-12-25 10:30:15.789012]]
    end
  end

  describe "timestamp_shorthand_to_value/1" do
    test "handles 'now'" do
      result = Helpers.timestamp_shorthand_to_value(["now"])

      assert %{shorthand: "now", value: datetime} = result
      assert %DateTime{} = datetime
      assert datetime.microsecond == {0, 0}
    end

    test "handles 'today'" do
      result = Helpers.timestamp_shorthand_to_value(["today"])

      assert %{shorthand: "today", value: {:range_operator, [start_dt, end_dt]}} = result
      assert %DateTime{hour: 0, minute: 0, second: 0} = start_dt

      # End should be start + 1 day - 1 second
      expected_end = start_dt |> Timex.shift(days: 1, seconds: -1)
      assert DateTime.compare(end_dt, expected_end) == :eq
    end

    test "handles 'yesterday'" do
      result = Helpers.timestamp_shorthand_to_value(["yesterday"])

      assert %{shorthand: "yesterday", value: {:range_operator, [start_dt, _end_dt]}} = result
      assert %DateTime{hour: 0, minute: 0, second: 0} = start_dt

      # Should be yesterday's date
      expected_start = Timex.today() |> Timex.shift(days: -1) |> Timex.to_datetime()
      assert DateTime.to_date(start_dt) == DateTime.to_date(expected_start)
    end

    test "handles 'this@minute'" do
      result = Helpers.timestamp_shorthand_to_value(["this", :minutes])

      assert %{shorthand: "this@minutes", value: {:range_operator, [start_dt, _end_dt]}} = result
      assert start_dt.second == 0
      assert start_dt.microsecond == {0, 0}
    end

    test "handles 'this@hour'" do
      result = Helpers.timestamp_shorthand_to_value(["this", :hours])

      assert %{shorthand: "this@hours", value: {:range_operator, [start_dt, _end_dt]}} = result
      assert start_dt.minute == 0
      assert start_dt.second == 0
    end

    test "handles 'last@5minute'" do
      result = Helpers.timestamp_shorthand_to_value(["last", 5, :minutes])

      assert %{shorthand: "last@5minutes", value: {:range_operator, [start_dt, end_dt]}} = result
      assert DateTime.diff(end_dt, start_dt, :minute) in 4..6
    end

    test "handles 'last@2week'" do
      result = Helpers.timestamp_shorthand_to_value(["last", 2, :weeks])

      assert %{shorthand: "last@2weeks", value: {:range_operator, [start_dt, end_dt]}} = result

      diff_days = DateTime.diff(end_dt, start_dt, :day)
      assert diff_days in 13..15
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

  describe "condition functions" do
    test "`not_quote` stops at unescaped quote" do
      assert Helpers.not_quote("\"test", [], 0, 0) == {:halt, []}
    end

    test "`not_quote` continues for escaped quote" do
      assert Helpers.not_quote("\\\"test", [], 0, 0) == {:cont, []}
    end

    test "`not_quote` continues for other characters" do
      assert Helpers.not_quote("atest", [], 0, 0) == {:cont, []}
      assert Helpers.not_quote("123test", [], 0, 0) == {:cont, []}
      assert Helpers.not_quote(" test", [], 0, 0) == {:cont, []}
      assert Helpers.not_quote("\\test", [], 0, 0) == {:cont, []}
    end

    test "`not_whitespace` stops at various whitespace characters" do
      assert Helpers.not_whitespace(" test", [], 0, 0) == {:halt, []}
      assert Helpers.not_whitespace("\ttest", [], 0, 0) == {:halt, []}
      assert Helpers.not_whitespace("\ntest", [], 0, 0) == {:halt, []}
      assert Helpers.not_whitespace("\vtest", [], 0, 0) == {:halt, []}
      assert Helpers.not_whitespace("\rtest", [], 0, 0) == {:halt, []}
      assert Helpers.not_whitespace("\ftest", [], 0, 0) == {:halt, []}
      assert Helpers.not_whitespace("\btest", [], 0, 0) == {:halt, []}
      assert Helpers.not_whitespace("", [], 0, 0) == {:halt, []}
    end

    test "`not_whitespace` continues for non-whitespace characters" do
      assert Helpers.not_whitespace("atest", [], 0, 0) == {:cont, []}
      assert Helpers.not_whitespace("1test", [], 0, 0) == {:cont, []}
      assert Helpers.not_whitespace("_test", [], 0, 0) == {:cont, []}
      assert Helpers.not_whitespace(".test", [], 0, 0) == {:cont, []}
    end

    test "`not_right_paren` stops at right parenthesis" do
      assert Helpers.not_right_paren(")test", [], 0, 0) == {:halt, []}
    end

    test "`not_right_paren` continues for other characters" do
      assert Helpers.not_right_paren("(test", [], 0, 0) == {:cont, []}
      assert Helpers.not_right_paren("atest", [], 0, 0) == {:cont, []}
      assert Helpers.not_right_paren("123test", [], 0, 0) == {:cont, []}
      assert Helpers.not_right_paren(" test", [], 0, 0) == {:cont, []}
    end
  end
end
