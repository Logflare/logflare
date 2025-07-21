defmodule Logflare.Lql.Parser.DateTimeParsers do
  @moduledoc """
  Date and time parsing combinators and helper functions
  """

  import NimbleParsec

  alias Logflare.DateTimeUtils
  alias Logflare.Lql.Parser.BasicCombinators

  def datetime_abbreviations() do
    choice([
      string("weeks") |> replace(:weeks),
      string("months") |> replace(:months),
      string("days") |> replace(:days),
      string("years") |> replace(:years),
      string("hours") |> replace(:hours),
      string("minutes") |> replace(:minutes),
      string("seconds") |> replace(:seconds),
      string("week") |> replace(:weeks),
      string("month") |> replace(:months),
      string("day") |> replace(:days),
      string("year") |> replace(:years),
      string("hour") |> replace(:hours),
      string("minute") |> replace(:minutes),
      string("second") |> replace(:seconds),
      string("mm") |> replace(:months),
      string("s") |> replace(:seconds),
      string("m") |> replace(:minutes),
      string("h") |> replace(:hours),
      string("d") |> replace(:days),
      string("w") |> replace(:weeks),
      string("y") |> replace(:years)
    ])
  end

  def date() do
    ascii_string([?0..?9], 4)
    |> string("-")
    |> ascii_string([?0..?9], 2)
    |> string("-")
    |> ascii_string([?0..?9], 2)
    |> reduce({Enum, :join, [""]})
    |> unwrap_and_tag(:date)
    |> label("ISO8601 date")
    |> reduce(:parse_date_or_datetime)
  end

  def datetime() do
    date()
    |> string("T")
    |> ascii_string([?0..?9], 2)
    |> string(":")
    |> ascii_string([?0..?9], 2)
    |> string(":")
    |> ascii_string([?0..?9], 2)
    |> optional(
      string(".")
      |> ascii_string([?0..?9], 6)
    )
    |> optional(
      choice([
        string("Z"),
        string("+")
        |> ascii_string([?0..?9], 2)
        |> string(":")
        |> ascii_string([?0..?9], 2)
      ])
    )
    |> reduce({Enum, :join, [""]})
    |> unwrap_and_tag(:datetime)
    |> label("ISO8601 datetime")
    |> reduce(:parse_date_or_datetime)
  end

  def integer_with_range(combinator \\ empty(), number) do
    combinator
    |> choice([
      ignore(string("{"))
      |> integer(number)
      |> ignore(string(".."))
      |> integer(number)
      |> ignore(string("}")),
      integer(number)
    ])
  end

  def datetime_with_range() do
    integer_with_range(4)
    |> tag(:year)
    |> ignore(string("-"))
    |> concat(
      integer_with_range(2)
      |> tag(:month)
    )
    |> ignore(string("-"))
    |> concat(
      integer_with_range(2)
      |> tag(:day)
    )
    |> optional(time_with_range())
    |> lookahead_not(string(".."))
    |> label("ISO8601 datetime with range")
    |> reduce(:parse_date_or_datetime_with_range)
    |> tag(:datetime_with_range)
  end

  def time_with_range() do
    ignore(string("T"))
    |> concat(
      integer_with_range(2)
      |> tag(:hour)
    )
    |> ignore(string(":"))
    |> concat(
      integer_with_range(2)
      |> tag(:minute)
    )
    |> ignore(string(":"))
    |> concat(
      integer_with_range(2)
      |> tag(:second)
    )
    |> lookahead_not(string(".."))
    |> optional(ignore(string(".")))
    |> concat(
      optional(
        choice([
          ignore(string("{"))
          |> ascii_string([?0..?9], min: 1, max: 6)
          |> ignore(string(".."))
          |> ascii_string([?0..?9], min: 1, max: 6)
          |> ignore(string("}")),
          ascii_string([?0..?9], min: 1, max: 6)
        ])
        |> tag(:microsecond)
      )
    )
    |> lookahead_not(string(".."))
    |> concat(
      optional(
        choice([
          ignore(string("Z")),
          string("+")
          |> ascii_string([?0..?9], 2)
          |> string(":")
          |> ascii_string([?0..?9], 2)
        ])
        # |> tag(:timezone)
      )
    )
  end

  def date_or_datetime() do
    [datetime(), date()]
    |> choice()
    |> label("date or datetime value")
  end

  def timestamp_shorthand_value() do
    choice([
      string("now"),
      string("today"),
      string("yesterday"),
      string("this")
      |> ignore(string("@"))
      |> concat(datetime_abbreviations()),
      string("last")
      |> ignore(string("@"))
      |> choice([integer(min: 1, max: 4), empty() |> replace(1)])
      |> concat(datetime_abbreviations())
    ])
    |> reduce(:timestamp_shorthand_to_value)
  end

  def timestamp_value() do
    choice([
      range_operator(date_or_datetime()),
      datetime_with_range(),
      date_or_datetime(),
      timestamp_shorthand_value(),
      invalid_match_all_value()
    ])
    |> unwrap_and_tag(:value)
    |> label("timestamp value")
  end

  # Helper functions that need to be available to the parsers
  @spec parse_date_or_datetime_with_range(list()) :: [Date.t() | NaiveDateTime.t()]
  def parse_date_or_datetime_with_range(result) when is_list(result) do
    [lv, rv] =
      result
      |> Enum.reduce([%{}, %{}], fn
        {k, v}, [lacc, racc] ->
          [lv, rv] =
            case v do
              [lv, rv] -> [lv, rv]
              [v] -> [v, v]
            end

          [Map.put(lacc, k, lv), Map.put(racc, k, rv)]
      end)
      |> Enum.map(fn
        %{second: _, minute: _, hour: _} = dt ->
          dt =
            Map.update(dt, :microsecond, {0, 0}, fn us ->
              {float, ""} = Float.parse("0." <> us)

              {round(float * 1_000_000), 6}
            end)

          struct!(NaiveDateTime, dt)

        d ->
          struct!(Date, d)
      end)

    if lv == rv do
      [lv]
    else
      [lv, rv]
    end
  end

  @spec parse_date_or_datetime([{:date | :datetime, String.t()}]) :: Date.t() | NaiveDateTime.t()
  def parse_date_or_datetime([{tag, result}]) do
    mod =
      case tag do
        :date -> Date
        :datetime -> NaiveDateTime
      end

    case mod.from_iso8601(result) do
      {:ok, dt, _offset} ->
        dt

      {:ok, dt} ->
        dt

      {:error, :invalid_format} ->
        throw(
          "Error while parsing timestamp #{tag} value: expected ISO8601 string, got '#{result}'"
        )

      {:error, e} ->
        throw(e)
    end
  end

  @spec timestamp_shorthand_to_value([String.t() | atom()]) :: %{
          shorthand: String.t(),
          value: term()
        }
  def timestamp_shorthand_to_value(["now"]) do
    %{value: %{Timex.now() | microsecond: {0, 0}}, shorthand: "now"}
  end

  def timestamp_shorthand_to_value(["today"]) do
    dt = Timex.today() |> Timex.to_datetime()
    value = {:range_operator, [dt, Timex.shift(dt, days: 1, seconds: -1)]}

    %{value: value, shorthand: "today"}
  end

  def timestamp_shorthand_to_value(["yesterday"]) do
    dt = Timex.today() |> Timex.shift(days: -1) |> Timex.to_datetime()
    value = {:range_operator, [dt, Timex.shift(dt, days: 1, seconds: -1)]}

    %{value: value, shorthand: "yesterday"}
  end

  def timestamp_shorthand_to_value(["this", period]) do
    now_ndt = DateTimeUtils.truncate(Timex.now(), :second)
    today_ndt = DateTimeUtils.truncate(now_ndt, :day)

    lvalue =
      case period do
        :minutes ->
          DateTimeUtils.truncate(now_ndt, :minute)

        :hours ->
          DateTimeUtils.truncate(now_ndt, :hour)

        :days ->
          today_ndt

        :weeks ->
          Timex.beginning_of_week(today_ndt)

        :months ->
          Timex.beginning_of_month(today_ndt)

        :years ->
          Timex.beginning_of_year(today_ndt)
      end

    value = {:range_operator, [lvalue, now_ndt]}
    %{value: value, shorthand: "this@#{period}"}
  end

  def timestamp_shorthand_to_value(["last", amount, period]) do
    amount = -amount

    now_ndt = DateTimeUtils.truncate(Timex.now(), :second)

    truncated =
      case period do
        :seconds ->
          now_ndt

        :minutes ->
          DateTimeUtils.truncate(now_ndt, :minute)

        :hours ->
          DateTimeUtils.truncate(now_ndt, :hour)

        :days ->
          DateTimeUtils.truncate(now_ndt, :day)

        :weeks ->
          DateTimeUtils.truncate(now_ndt, :day)

        :months ->
          DateTimeUtils.truncate(now_ndt, :day)

        :years ->
          DateTimeUtils.truncate(now_ndt, :day)
      end

    lvalue = Timex.shift(truncated, [{period, amount}])
    value = {:range_operator, [lvalue, now_ndt]}
    %{value: value, shorthand: "last@#{if amount < 0, do: -amount, else: amount}#{period}"}
  end

  defdelegate range_operator(combinator), to: BasicCombinators
  defdelegate invalid_match_all_value(), to: BasicCombinators
end
