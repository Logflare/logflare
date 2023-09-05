defmodule Logflare.Lql.Parser.Helpers do
  @moduledoc """
  Includes parsers and combinators for Lql parser
  """
  import NimbleParsec
  alias Logflare.Lql.FilterRule
  alias Logflare.DateTimeUtils
  @isolated_string :isolated_string

  def word do
    optional(
      string("~")
      |> replace(:"~")
      |> unwrap_and_tag(:operator)
    )
    |> concat(
      times(
        choice([
          string(~S(\")),
          ascii_char([
            ?a..?z,
            ?A..?Z,
            ?.,
            ?_,
            ?0..?9,
            ?!,
            ?%,
            ?$,
            ?^,
            ?\\,
            ?+,
            ?[,
            ?],
            ??,
            ?!,
            ?(,
            ?),
            ?{,
            ?}
          ])
        ]),
        min: 1
      )
      |> reduce({List, :to_string, []})
      |> unwrap_and_tag(:word)
    )
    |> label("word filter")
    |> reduce({:to_rule, [:event_message]})
  end

  def quoted_string(location \\ :quoted_field_value)
      when location in [:quoted_event_message, :quoted_field_value] do
    optional(
      string("~")
      |> replace(:"~")
      |> unwrap_and_tag(:operator)
    )
    |> concat(
      ignore(string("\""))
      |> repeat_while(
        choice([
          string(~S(\")),
          utf8_char([])
        ]),
        {:not_quote, []}
      )
      |> ignore(string("\""))
      |> reduce({List, :to_string, []})
      |> unwrap_and_tag(@isolated_string)
    )
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
    |> reduce({:to_rule, [:filter_maybe_shorthand]})
    |> reduce(:apply_value_modifiers)
    |> map({:check_for_no_invalid_metadata_field_values, [:timestamp]})
    |> label("timestamp filter rule clause")
  end

  def metadata_clause do
    metadata_field()
    |> concat(operator())
    |> concat(field_value())
    |> reduce({:to_rule, [:filter]})
    |> reduce(:apply_value_modifiers)
    |> map({:check_for_no_invalid_metadata_field_values, [:metadata]})
    |> label("metadata filter rule clause")
  end

  def field_clause do
    any_field()
    |> concat(operator())
    |> concat(field_value())
    |> reduce({:to_rule, [:filter]})
    |> reduce(:apply_value_modifiers)
    |> map({:check_for_no_invalid_metadata_field_values, [:metadata]})
    |> label("field filter rule clause")
  end

  def chart_clause() do
    ignore(choice([string("chart"), string("c")]))
    |> ignore(ascii_char([?:]))
    |> choice([
      chart_aggregate_group_by(),
      chart_aggregate()
    ])
    |> tag(:chart)
  end

  def chart_aggregate() do
    choice([
      string("avg") |> replace(:avg),
      string("count") |> replace(:count),
      string("sum") |> replace(:sum),
      string("max") |> replace(:max),
      string("p50") |> replace(:p50),
      string("p95") |> replace(:p95),
      string("p99") |> replace(:p99)
    ])
    |> unwrap_and_tag(:aggregate)
    |> ignore(string("("))
    |> concat(
      choice([string("*") |> replace("timestamp") |> unwrap_and_tag(:path), metadata_field()])
    )
    |> ignore(string(")"))
  end

  def chart_aggregate_group_by() do
    ignore(string("group_by"))
    |> ignore(string("("))
    |> ignore(choice([string("timestamp"), string("t")]))
    |> ignore(string("::"))
    |> choice([
      string("second") |> replace(:second),
      string("s") |> replace(:second),
      string("minute") |> replace(:minute),
      string("m") |> replace(:minute),
      string("hour") |> replace(:hour),
      string("h") |> replace(:hour),
      string("day") |> replace(:day),
      string("d") |> replace(:day)
    ])
    |> unwrap_and_tag(:period)
    |> ignore(string(")"))
  end

  def any_field do
    ascii_string([?a..?z, ?A..?Z, ?., ?_, ?0..?9], min: 1)
    |> reduce({List, :to_string, []})
    |> unwrap_and_tag(:path)
    |> label("schema field")
  end

  def metadata_field do
    choice([string("metadata"), string("m") |> replace("metadata")])
    |> string(".")
    |> ascii_string([?a..?z, ?A..?Z, ?., ?_, ?0..?9], min: 2)
    |> reduce({List, :to_string, []})
    |> unwrap_and_tag(:path)
    |> label("metadata field")
  end

  @list_includes_op :list_includes
  def operator() do
    choice([
      string(":>=") |> replace(:>=),
      string(":<=") |> replace(:<=),
      string(":>") |> replace(:>),
      string(":<") |> replace(:<),
      string(":~") |> replace(:"~"),
      string(":@>") |> replace(@list_includes_op),
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

  def field_value do
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
      datetime_with_range(),
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

  @spec apply_value_modifiers([FilterRule.t()]) :: FilterRule.t()
  def apply_value_modifiers([rule]) do
    case rule.value do
      {:range_operator, [lvalue, rvalue]} ->
        lvalue =
          case lvalue do
            {:datetime_with_range, [[x]]} -> x
            x -> x
          end

        rvalue =
          case rvalue do
            {:datetime_with_range, [[x]]} -> x
            x -> x
          end

        %{rule | values: [lvalue, rvalue], operator: :range, value: nil}

      _ ->
        rule
    end
  end

  def maybe_apply_negation_modifier([:negate, rules]) when is_list(rules) do
    Enum.map(rules, &maybe_apply_negation_modifier([:negate, &1]))
  end

  def maybe_apply_negation_modifier([:negate, rule]) do
    update_in(rule.modifiers, &Map.put(&1, :negate, true))
  end

  def maybe_apply_negation_modifier(rule), do: rule

  def level_strings() do
    choice([
      string("debug") |> replace(0),
      string("info") |> replace(1),
      string("notice") |> replace(2),
      string("warning") |> replace(3),
      string("error") |> replace(4),
      string("critical") |> replace(5),
      string("alert") |> replace(6),
      string("emergency") |> replace(7)
    ])
  end

  def metadata_level_clause() do
    string("metadata.level")
    |> ignore(string(":"))
    |> concat(range_operator(level_strings()))
    |> tag(:metadata_level_clause)
    |> reduce(:to_rule)
  end

  @level_orders %{
    0 => "debug",
    1 => "info",
    2 => "notice",
    3 => "warning",
    4 => "error",
    5 => "critical",
    6 => "alert",
    7 => "emergency"
  }
  def to_rule(metadata_level_clause: ["metadata.level", {:range_operator, [left, right]}]) do
    left..right
    |> Enum.map(&Map.get(@level_orders, &1))
    |> Enum.map(fn level ->
      %FilterRule{
        path: "metadata.level",
        operator: :=,
        value: level,
        modifiers: %{}
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
      operator: args[:operator] || :string_contains,
      modifiers: %{quoted_string: true}
    }
  end

  def to_rule(args, :event_message) do
    %FilterRule{
      path: "event_message",
      value: args[:word],
      operator: args[:operator] || :string_contains
    }
  end

  def to_rule(args, :filter_maybe_shorthand) do
    args =
      case args[:value] do
        %{shorthand: sh, value: value} ->
          sh =
            case sh do
              "this@" <> _ -> String.trim_trailing(sh, "s")
              "last@" <> _ -> String.trim_trailing(sh, "s")
              _ -> sh
            end

          args
          |> Keyword.replace!(:value, value)
          |> Keyword.put(:shorthand, sh)

        _ ->
          args
      end

    to_rule(args, :filter)
  end

  def to_rule(args, :filter) when is_list(args) do
    filter = struct!(FilterRule, Map.new(args))

    cond do
      match?({:quoted, _}, filter.value) ->
        {:quoted, value} = filter.value

        filter
        |> Map.update!(:modifiers, &Map.put(&1, :quoted_string, true))
        |> Map.put(:value, value)

      match?(%{value: {:datetime_with_range, _}}, filter) ->
        {_, value} = filter.value

        case value do
          [[_, _] = v] ->
            %{filter | value: nil, values: v, operator: :range}

          [[v]] ->
            %{filter | value: v}
        end

      true ->
        filter
    end
  end

  def check_for_no_invalid_metadata_field_values(
        %{path: _p, value: {:invalid_metadata_field_value, v}},
        :timestamp
      ) do
    throw(
      "Error while parsing timestamp filter value: expected ISO8601 string or range or shorthand, got '#{v}'"
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

  def not_quote(<<?\\, ?", _::binary>>, context, _, _), do: {:cont, context}
  def not_quote(<<?", _::binary>>, context, _, _), do: {:halt, context}
  def not_quote(_, context, _, _), do: {:cont, context}

  def not_whitespace(<<c, _::binary>>, context, _, _)
      when c in [?\s, ?\n, ?\t, ?\v, ?\r, ?\f, ?\b],
      do: {:halt, context}

  def not_whitespace("", context, _, _), do: {:halt, context}
  def not_whitespace(_, context, _, _), do: {:cont, context}

  def not_right_paren(<<?), _::binary>>, context, _, _), do: {:halt, context}
  def not_right_paren(_, context, _, _), do: {:cont, context}

  def get_level_order(level) do
    @level_orders
    |> Enum.map(fn {k, v} ->
      {v, k}
    end)
    |> Map.new()
    |> Map.get(level)
  end

  def get_level_by_order(level) do
    @level_orders[level]
  end
end
