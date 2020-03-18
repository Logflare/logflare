defmodule Logflare.Lql.Parser.Helpers do
  @moduledoc """
  Includes parsers and combinators for Lql parser
  """
  import NimbleParsec
  alias Logflare.Lql.FilterRule
  alias Logflare.Lql.ChartRule
  @isolated_string :isolated_string

  def word do
    [?a..?z, ?A..?Z, ?., ?_, ?0..?9]
    |> ascii_string(min: 1)
    |> unwrap_and_tag(:word)
    |> label("word filter")
    |> reduce({:to_rule, [:event_message]})
  end

  def quoted_string(location \\ :quoted_field_value)
      when location in [:quoted_event_message, :quoted_field_value] do
    ignore(string("\""))
    |> repeat_while(
      utf8_char([]),
      {:not_quote, []}
    )
    |> ignore(string("\""))
    |> reduce({List, :to_string, []})
    |> unwrap_and_tag(@isolated_string)
    |> label("quoted string filter")
    |> reduce({:to_rule, [location]})
  end

  def parens_string() do
    ignore(string("("))
    |> repeat_while(
      utf8_char([]),
      {:not_right_paren, []}
    )
    |> ignore(string(")"))
    |> reduce({List, :to_string, []})
    |> unwrap_and_tag(@isolated_string)
    |> label("parens string")
    |> reduce({:to_rule, [:quoted_field_value]})
  end

  def timestamp_clause() do
    choice([string("timestamp"), string("t")])
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
    ignore(choice([string("chart"), string("c")]))
    |> ignore(ascii_char([?:]))
    |> choice([
      chart_aggregate()
      |> label("chart_aggregate"),
      chart_period()
      |> label("chart_period"),
      metadata_field()
      |> label("chart field")
    ])
    |> unwrap_and_tag(:chart)
  end

  def chart_period() do
    ignore(string("period"))
    |> ignore(string("@"))
    |> choice([
      string("second") |> replace(:second),
      string("s") |> replace(:second),
      string("minute") |> replace(:minute),
      string("m") |> replace(:minute),
      string("h") |> replace(:hour),
      string("hour") |> replace(:hour),
      string("d") |> replace(:day),
      string("day") |> replace(:day)
    ])
    |> unwrap_and_tag(:period)
  end

  def chart_aggregate() do
    ignore(string("aggregate"))
    |> ignore(string("@"))
    |> choice([
      string("sum") |> replace(:sum),
      string("avg") |> replace(:avg),
      string("count") |> replace(:count)
    ])
    |> unwrap_and_tag(:aggregate)
  end

  def metadata_field do
    choice([string("metadata"), string("m") |> replace("metadata")])
    |> string(".")
    |> ascii_string([?a..?z, ?A..?Z, ?., ?_, ?0..?9], min: 2)
    |> reduce({List, :to_string, []})
    |> unwrap_and_tag(:path)
    |> label("metadata BQ field")
  end

  @list_includes_op :list_includes
  def operator() do
    choice([
      string(":>=") |> replace(:>=),
      string(":<=") |> replace(:<=),
      string(":>") |> replace(:>),
      string(":<") |> replace(:<),
      string(":~") |> replace(:"~"),
      choice([string(":@>"), string(":_includes")]) |> replace(@list_includes_op),
      # string(":") should always be the last choice
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
      null(),
      quoted_string(),
      parens_string(),
      ascii_string([?a..?z, ?A..?Z, ?_, ?0..?9], min: 1),
      invalid_match_all_value()
    ])
    |> unwrap_and_tag(:value)
    |> label("valid filter value")
  end

  def null() do
    string("NULL") |> replace(:NULL)
  end

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

  def timestamp_shorthand_value do
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

  def timestamp_shorthand_to_value(["now"]), do: %{Timex.now() | microsecond: {0, 0}}

  def timestamp_shorthand_to_value(["today"]) do
    dt = Timex.today() |> Timex.to_datetime()
    {:range_operator, [dt, Timex.shift(dt, days: 1, seconds: -1)]}
  end

  def timestamp_shorthand_to_value(["yesterday"]) do
    dt = Timex.today() |> Timex.shift(days: -1) |> Timex.to_datetime()
    {:range_operator, [dt, Timex.shift(dt, days: 1, seconds: -1)]}
  end

  def timestamp_shorthand_to_value(["this", period]) do
    now_ndt = %{Timex.now() | microsecond: {0, 0}, second: 0}
    now_ndt_no_m = %{now_ndt | minute: 0}
    now_ndt_no_h = %{now_ndt_no_m | hour: 0}

    case period do
      :minutes ->
        {:range_operator, [now_ndt, now_ndt]}

      :hours ->
        {:range_operator, [now_ndt_no_m, now_ndt]}

      :days ->
        {:range_operator, [now_ndt_no_h, now_ndt]}

      :weeks ->
        {:range_operator, [Timex.beginning_of_week(now_ndt_no_h), now_ndt]}

      :months ->
        {:range_operator, [Timex.beginning_of_month(now_ndt_no_h), now_ndt]}

      :years ->
        {:range_operator, [Timex.beginning_of_year(now_ndt_no_h), now_ndt]}
    end
  end

  def timestamp_shorthand_to_value(["last", amount, period]) do
    amount = -amount
    now_ndt_with_seconds = %{Timex.now() | microsecond: {0, 0}}
    now_ndt = %{Timex.now() | microsecond: {0, 0}, second: 0}
    now_ndt_no_m = %{now_ndt | minute: 0}
    now_ndt_no_h = %{now_ndt_no_m | hour: 0}

    case period do
      :seconds ->
        {:range_operator,
         [Timex.shift(now_ndt_with_seconds, [{period, amount}]), now_ndt_with_seconds]}

      :minutes ->
        {:range_operator, [Timex.shift(now_ndt, [{period, amount}]), now_ndt]}

      :hours ->
        {:range_operator, [Timex.shift(now_ndt_no_m, [{period, amount}]), now_ndt]}

      :days ->
        {:range_operator, [Timex.shift(now_ndt_no_h, [{period, amount}]), now_ndt]}

      :weeks ->
        {:range_operator, [Timex.shift(now_ndt_no_h, [{:days, amount * 7}]), now_ndt]}

      :months ->
        {:range_operator,
         [
           Timex.shift(%{now_ndt_no_h | day: 1}, [{period, amount}]),
           now_ndt
         ]}

      :years ->
        {:range_operator, [Timex.shift(now_ndt_no_h, [{period, amount}]), now_ndt]}
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

  def level_strings() do
    choice([
      string("debug") |> replace(0),
      string("info") |> replace(1),
      string("warning") |> replace(2),
      string("error") |> replace(3)
    ])
  end

  def metadata_level_clause() do
    string("metadata.level")
    |> ignore(string(":"))
    |> concat(range_operator(level_strings()))
    |> tag(:metadata_level_clause)
    |> reduce(:to_rule)
  end

  @level_orders %{0 => "debug", 1 => "info", 2 => "warning", 3 => "error"}
  def to_rule(metadata_level_clause: ["metadata.level", {:range_operator, [left, right]}]) do
    left..right
    |> Enum.map(&Map.get(@level_orders, &1))
    |> Enum.map(fn level ->
      %FilterRule{
        path: "metadata.level",
        operator: :=,
        value: level,
        modifiers: []
      }
    end)
  end

  def to_rule(args, :quoted_field_value) do
    {:quoted, args[@isolated_string]}
  end

  def to_rule(args, :quoted_event_message) do
    %FilterRule{
      path: "event_message",
      value: args[@isolated_string],
      operator: :"~",
      modifiers: [:quoted_string]
    }
  end

  def to_rule(args, :event_message) do
    %FilterRule{
      path: "event_message",
      value: args[:word],
      operator: :"~"
    }
  end

  def to_rule(args, :filter) when is_list(args) do
    filter = struct!(FilterRule, Map.new(args))

    args =
      if match?({:quoted, _}, filter.value) do
        {:quoted, value} = filter.value

        filter
        |> Map.update!(:modifiers, &[:quoted_string | &1])
        |> Map.put(:value, value)
      else
        filter
      end
  end

  def check_for_no_invalid_metadata_field_values(
        %{path: _p, value: {:invalid_metadata_field_value, v}},
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

  def not_right_paren(<<?), _::binary>>, context, _, _), do: {:halt, context}
  def not_right_paren(_, context, _, _), do: {:cont, context}

  def get_level_order(level) do
    @level_orders
    |> Enum.map(fn {k,v} ->
      {v, k}
    end)
    |> Map.new
  |> Map.get(level)
  end

  def get_level_by_order(level) do
    @level_orders[level]
  end
end
