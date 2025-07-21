defmodule Logflare.Lql.Parser.DateTimeParsersTest do
  use ExUnit.Case, async: true

  import NimbleParsec

  alias Logflare.Lql.Parser.DateTimeParsers

  defparsec(:test_datetime_abbreviations, DateTimeParsers.datetime_abbreviations())

  describe "parse_date_or_datetime/1" do
    test "parses valid date" do
      input = [{:date, "2023-12-25"}]
      result = DateTimeParsers.parse_date_or_datetime(input)
      assert result == ~D[2023-12-25]
    end

    test "parses valid datetime without timezone" do
      input = [{:datetime, "2023-12-25T14:30:45"}]
      result = DateTimeParsers.parse_date_or_datetime(input)
      assert result == ~N[2023-12-25 14:30:45]
    end

    test "parses valid datetime with Z timezone" do
      input = [{:datetime, "2023-12-25T14:30:45Z"}]
      result = DateTimeParsers.parse_date_or_datetime(input)
      assert result == ~N[2023-12-25 14:30:45]
    end

    test "parses valid datetime with microseconds" do
      input = [{:datetime, "2023-12-25T14:30:45.123456"}]
      result = DateTimeParsers.parse_date_or_datetime(input)
      assert result == ~N[2023-12-25 14:30:45.123456]
    end

    test "parses valid datetime with offset" do
      input = [{:datetime, "2023-12-25T14:30:45+02:00"}]
      result = DateTimeParsers.parse_date_or_datetime(input)
      assert result == ~N[2023-12-25 14:30:45]
    end

    test "throws error for invalid date format" do
      input = [{:date, "invalid-date"}]

      assert catch_throw(DateTimeParsers.parse_date_or_datetime(input)) =~
               "Error while parsing timestamp date value: expected ISO8601 string, got 'invalid-date'"
    end

    test "throws error for invalid datetime format" do
      input = [{:datetime, "2023-13-45T25:70:90"}]

      result = catch_throw(DateTimeParsers.parse_date_or_datetime(input))
      assert result == :invalid_date
    end

    test "handles leap year dates" do
      input = [{:date, "2024-02-29"}]
      result = DateTimeParsers.parse_date_or_datetime(input)
      assert result == ~D[2024-02-29]
    end

    test "throws error for invalid leap year" do
      input = [{:date, "2023-02-29"}]

      result = catch_throw(DateTimeParsers.parse_date_or_datetime(input))
      assert result == :invalid_date
    end
  end

  describe "parse_date_or_datetime_with_range/1" do
    test "creates date range" do
      input = [year: [2023, 2024], month: [1, 2], day: [1, 15]]
      result = DateTimeParsers.parse_date_or_datetime_with_range(input)

      assert result == [~D[2023-01-01], ~D[2024-02-15]]
    end

    test "creates single date when values are the same" do
      input = [year: [2023], month: [12], day: [25]]
      result = DateTimeParsers.parse_date_or_datetime_with_range(input)

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

      result = DateTimeParsers.parse_date_or_datetime_with_range(input)

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

      result = DateTimeParsers.parse_date_or_datetime_with_range(input)

      assert result == [~N[2023-12-25 10:30:15.123456], ~N[2023-12-25 10:30:15.789012]]
    end
  end

  describe "timestamp_shorthand_to_value/1" do
    test "handles 'now'" do
      result = DateTimeParsers.timestamp_shorthand_to_value(["now"])

      assert %{shorthand: "now", value: datetime} = result
      assert %DateTime{} = datetime
      assert datetime.microsecond == {0, 0}
    end

    test "handles 'today'" do
      result = DateTimeParsers.timestamp_shorthand_to_value(["today"])

      assert %{shorthand: "today", value: {:range_operator, [start_dt, end_dt]}} = result
      assert %DateTime{hour: 0, minute: 0, second: 0} = start_dt

      # End should be start + 1 day - 1 second
      expected_end = Timex.shift(start_dt, days: 1, seconds: -1)
      assert DateTime.compare(end_dt, expected_end) == :eq
    end

    test "handles 'yesterday'" do
      result = DateTimeParsers.timestamp_shorthand_to_value(["yesterday"])

      assert %{shorthand: "yesterday", value: {:range_operator, [start_dt, _end_dt]}} = result
      assert %DateTime{hour: 0, minute: 0, second: 0} = start_dt

      # Should be yesterday's date
      expected_start = Timex.today() |> Timex.shift(days: -1) |> Timex.to_datetime()
      assert DateTime.to_date(start_dt) == DateTime.to_date(expected_start)
    end

    test "handles 'this@minute'" do
      result = DateTimeParsers.timestamp_shorthand_to_value(["this", :minutes])

      assert %{shorthand: "this@minutes", value: {:range_operator, [start_dt, _end_dt]}} = result
      assert start_dt.second == 0
      assert start_dt.microsecond == {0, 0}
    end

    test "handles 'this@hour'" do
      result = DateTimeParsers.timestamp_shorthand_to_value(["this", :hours])

      assert %{shorthand: "this@hours", value: {:range_operator, [start_dt, _end_dt]}} = result
      assert start_dt.minute == 0
      assert start_dt.second == 0
    end

    test "handles 'last@5minute'" do
      result = DateTimeParsers.timestamp_shorthand_to_value(["last", 5, :minutes])

      assert %{shorthand: "last@5minutes", value: {:range_operator, [start_dt, end_dt]}} = result
      assert DateTime.diff(end_dt, start_dt, :minute) in 4..6
    end

    test "handles 'last@2week'" do
      result = DateTimeParsers.timestamp_shorthand_to_value(["last", 2, :weeks])

      assert %{shorthand: "last@2weeks", value: {:range_operator, [start_dt, end_dt]}} = result

      diff_days = DateTime.diff(end_dt, start_dt, :day)
      assert diff_days in 13..15
    end
  end

  describe "datetime abbreviations parser" do
    test "parses full and abbreviated time periods" do
      assert {:ok, [:seconds], "", _, _, _} = test_datetime_abbreviations("seconds")
      assert {:ok, [:minutes], "", _, _, _} = test_datetime_abbreviations("minutes")
      assert {:ok, [:hours], "", _, _, _} = test_datetime_abbreviations("hours")
      assert {:ok, [:days], "", _, _, _} = test_datetime_abbreviations("days")
      assert {:ok, [:weeks], "", _, _, _} = test_datetime_abbreviations("weeks")
      assert {:ok, [:months], "", _, _, _} = test_datetime_abbreviations("months")
      assert {:ok, [:years], "", _, _, _} = test_datetime_abbreviations("years")
      assert {:ok, [:seconds], "", _, _, _} = test_datetime_abbreviations("s")
      assert {:ok, [:minutes], "", _, _, _} = test_datetime_abbreviations("m")
      assert {:ok, [:hours], "", _, _, _} = test_datetime_abbreviations("h")
      assert {:ok, [:days], "", _, _, _} = test_datetime_abbreviations("d")
      assert {:ok, [:weeks], "", _, _, _} = test_datetime_abbreviations("w")
      assert {:ok, [:years], "", _, _, _} = test_datetime_abbreviations("y")
    end
  end
end
