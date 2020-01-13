defmodule Logflare.Logs.Search.Parser.Helpers do
  import NimbleParsec
  alias Logflare.Lql.FilterRule
  alias Logflare.Lql.ChartRule

  def word do
    [?a..?z, ?A..?Z, ?., ?_, ?0..?9]
    |> ascii_string(min: 1)
    |> unwrap_and_tag(:word)
    |> label("word filter")
    |> reduce({:to_rule, [:event_message]})
  end

  def quoted_string(path \\ :ignore) when path in [:event_message, :ignore] do
    ignore(ascii_char([?"]))
    |> repeat_while(
      utf8_char([]),
      {:not_quote, []}
    )
    |> ignore(ascii_char([?"]))
    |> reduce({List, :to_string, []})
    |> unwrap_and_tag(:quoted_string)
    |> label("quoted string filter")
    |> reduce({:to_rule, [path]})
  end

  def timestamp_clause() do
    string("timestamp")
    |> replace({:path, "timestamp"})
    |> concat(operator())
    |> concat(timestamp_value())
    |> reduce({:to_rule, [:filter]})
    |> reduce(:apply_value_modifiers)
    |> map({:check_for_no_invalid_metadata_field_values, [:timestamp]})
    |> label("timestamp filter rule clause")
  end

  def metadata_clause do
    metadata_field()
    |> concat(operator())
    |> concat(metadata_field_value())
    |> reduce({:to_rule, [:filter]})
    |> reduce(:apply_value_modifiers)
    |> map({:check_for_no_invalid_metadata_field_values, [:metadata]})
    |> label("metadata filter rule clause")
  end

  def chart_clause() do
    string("chart")
    |> ignore(ascii_char([?:]))
    |> concat(metadata_field())
    |> tag(:chart_clause)
    |> label("chart clause")
    |> reduce({:to_rule, []})
  end

  def metadata_field do
    string("metadata")
    |> string(".")
    |> ascii_string([?a..?z, ?A..?Z, ?., ?_, ?0..?9], min: 2)
    |> reduce({List, :to_string, []})
    |> unwrap_and_tag(:path)
    |> label("metadata BQ field")
  end

  def operator() do
    choice([
      string(":>=") |> replace(:>=),
      string(":<=") |> replace(:<=),
      string(":>") |> replace(:>),
      string(":<") |> replace(:<),
      string(":~") |> replace(:"~"),
      string(":") |> replace(:=)
    ])
    |> unwrap_and_tag(:operator)
    |> label("filter operator")
  end

  def number() do
    ascii_string([?0..?9], min: 1)
    |> concat(
      optional(
        string(".")
        |> ascii_string([?0..?9], min: 1)
      )
    )
    |> reduce({Enum, :join, [""]})
    |> label("number")
  end

  def metadata_field_value do
    choice([
      range_operator(number()),
      number(),
      quoted_string(),
      ascii_string([?a..?z, ?A..?Z, ?_], min: 1),
      invalid_match_all_value()
    ])
    |> unwrap_and_tag(:value)
    |> label("valid filter value")
  end

  def datetime_abbreviations_choice() do
    choice([
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

  def timestamp_shorthand_value do
    choice([
      string("now"),
      string("today"),
      string("yesterday"),
      string("this")
      |> ignore(string("@"))
      |> concat(datetime_abbreviations_choice()),
      string("last")
      |> ignore(string("@"))
      |> optional(integer(min: 1, max: 3))
      |> concat(datetime_abbreviations_choice())
    ])
    |> reduce(:timestamp_shorthand_to_value)
  end

  def timestamp_shorthand_to_value(["now"]), do: %{Timex.now() | microsecond: {0, 0}}
  def timestamp_shorthand_to_value(["today"]), do: Timex.today()
  def timestamp_shorthand_to_value(["yesterday"]), do: Timex.today() |> Timex.shift(days: -1)

  def timestamp_shorthand_to_value(["this", period]) do
    now_ndt = %{Timex.now() | microsecond: {0, 0}, second: 0}

    case period do
      :minutes ->
        {:range_operator, [now_ndt, now_ndt]}

      :hours ->
        {:range_operator, [%{now_ndt | minute: 0}, now_ndt]}

      :days ->
        {:range_operator, [%{now_ndt | hour: 0, minute: 0}, now_ndt]}

      :weeks ->
        {:range_operator,
         [
           Timex.beginning_of_week(%{now_ndt | hour: 0, minute: 0}),
           now_ndt
         ]}

      :months ->
        {:range_operator, [Timex.beginning_of_month(%{now_ndt | hour: 0, minute: 0}), now_ndt]}

      :years ->
        {:range_operator, [Timex.beginning_of_year(%{now_ndt | hour: 0, minute: 0}), now_ndt]}
    end
  end

  def timestamp_shorthand_to_value(["last", amount, period]) do
    amount = -amount
    now_ndt_with_seconds = %{Timex.now() | microsecond: {0, 0}}

    now_ndt = %{Timex.now() | microsecond: {0, 0}, second: 0}

    case period do
      :seconds ->
        {:range_operator,
         [Timex.shift(now_ndt_with_seconds, [{period, amount}]), now_ndt_with_seconds]}

      :minutes ->
        {:range_operator, [Timex.shift(now_ndt, [{period, amount}]), now_ndt]}

      :hours ->
        {:range_operator, [Timex.shift(%{now_ndt | minute: 0}, [{period, amount}]), now_ndt]}

      :days ->
        {:range_operator,
         [Timex.shift(%{now_ndt | hour: 0, minute: 0}, [{period, amount}]), now_ndt]}

      :weeks ->
        {:range_operator,
         [Timex.shift(%{now_ndt | hour: 0, minute: 0}, [{:days, amount * 7}]), now_ndt]}

      :months ->
        {:range_operator,
         [
           Timex.shift(%{now_ndt | hour: 0, minute: 0, day: 1}, [{period, amount}]),
           now_ndt
         ]}

      :years ->
        {:range_operator,
         [
           Timex.shift(
             %{now_ndt | hour: 0, minute: 0},
             [{period, amount}]
           ),
           now_ndt
         ]}
    end
  end

  def parse_date_or_datetime([{tag, result}]) do
    mod =
      case tag do
        :date -> Date
        :datetime -> DateTime
      end

    case mod.from_iso8601(result) do
      {:ok, dt, _offset} ->
        dt

      {:ok, dt} ->
        dt

      {:error, :invalid_format} ->
        throw(
          "Error while parsing timestamp #{tag} value: expected ISO8601 string, got #{result}"
        )

      {:error, e} ->
        throw(e)
    end
  end

  def date do
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

  def datetime do
    date()
    |> string("T")
    |> ascii_string([?0..?9], 2)
    |> string(":")
    |> ascii_string([?0..?9], 2)
    |> string(":")
    |> ascii_string([?0..?9], 2)
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

  def date_or_datetime do
    [datetime(), date()]
    |> choice()
    |> label("date or datetime value")
  end

  def range_operator(combinator) do
    combinator
    |> concat(ignore(string("..")))
    |> concat(combinator)
    |> label("range operator")
    |> tag(:range_operator)
  end

  def timestamp_value() do
    choice([
      range_operator(date_or_datetime()),
      date_or_datetime(),
      timestamp_shorthand_value(),
      invalid_match_all_value()
    ])
    |> unwrap_and_tag(:value)
    |> label("timestamp value")
  end

  def invalid_match_all_value do
    choice([
      ascii_string([33..255], min: 1),
      empty() |> replace(~S|""|)
    ])
    |> unwrap_and_tag(:invalid_metadata_field_value)
  end

  def apply_value_modifiers([rule]) do
    case rule.value do
      {:range_operator, [lvalue, rvalue]} ->
        [
          %FilterRule{
            path: rule.path,
            value: lvalue,
            operator: :>=
          },
          %FilterRule{
            path: rule.path,
            value: rvalue,
            operator: :<=
          }
        ]

      _ ->
        rule
    end
  end

  def maybe_apply_negation_modifier([:negate, rule]) do
    modifiers = [:negate | rule.modifiers]
    %{rule | modifiers: modifiers}
  end

  def maybe_apply_negation_modifier(rule), do: rule

  def to_rule(chart_clause: chart_clause) do
    %ChartRule{
      path: chart_clause[:path],
      value_type: nil
    }
  end

  def to_rule(args, :ignore), do: args[:quoted_string]

  def to_rule(args, :event_message) do
    value = args[:quoted_string] || args[:word]

    %FilterRule{
      path: "event_message",
      value: value,
      operator: :"~"
    }
  end

  def to_rule(args, :filter) when is_list(args) do
    struct!(FilterRule, Map.new(args))
  end

  def check_for_no_invalid_metadata_field_values(
        %{path: p, value: {:invalid_metadata_field_value, v}},
        :timestamp
      ) do
    throw(
      "Error while parsing timestamp filter value: expected ISO8601 string or range, got #{v}"
    )
  end

  def check_for_no_invalid_metadata_field_values(
        %{path: p, value: {:invalid_metadata_field_value, v}},
        :metadata
      ) do
    throw("Error while parsing `#{p}` field metadata filter value: #{v}")
  end

  def check_for_no_invalid_metadata_field_values(rule, _) do
    rule
  end

  def not_quote(<<?", _::binary>>, context, _, _), do: {:halt, context}
  def not_quote(_, context, _, _), do: {:cont, context}
end
