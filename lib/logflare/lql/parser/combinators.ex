defmodule Logflare.Lql.Parser.Combinators do
  @moduledoc """
  All `NimbleParsec` combinators for LQL parsing.

  This module contains all the parser combinators that define the grammar
  for the Logflare Query Language (LQL).
  """

  import NimbleParsec

  @isolated_string :isolated_string
  @list_includes_op :list_includes
  @list_includes_regex_op :list_includes_regexp

  # ============================================================================
  # Basic Combinators
  # ============================================================================

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

  def parens_string do
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

  def operator do
    choice([
      string(":>=") |> replace(:>=),
      string(":<=") |> replace(:<=),
      string(":>") |> replace(:>),
      string(":<") |> replace(:<),
      string(":~") |> replace(:"~"),
      string(":@>~") |> replace(@list_includes_regex_op),
      string(":@>") |> replace(@list_includes_op),
      # string(":") should always be the last choice
      string(":") |> replace(:=)
    ])
    |> unwrap_and_tag(:operator)
    |> label("filter operator")
  end

  def number do
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

  def null do
    string("NULL") |> replace(:NULL)
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

  def invalid_match_all_value do
    choice([
      ascii_string([33..255], min: 1),
      empty() |> replace(~S|""|)
    ])
    |> unwrap_and_tag(:invalid_metadata_field_value)
  end

  def range_operator(combinator) do
    combinator
    |> concat(ignore(string("..")))
    |> concat(combinator)
    |> label("range operator")
    |> tag(:range_operator)
  end

  def level_strings do
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

  # ============================================================================
  # Chart Combinators
  # ============================================================================

  def chart_clause do
    ignore(choice([string("chart"), string("c")]))
    |> ignore(ascii_char([?:]))
    |> choice([
      chart_aggregate_group_by(),
      chart_aggregate()
    ])
    |> tag(:chart)
  end

  def chart_aggregate do
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
      choice([
        string("*") |> replace("timestamp") |> unwrap_and_tag(:path),
        metadata_field(),
        any_field()
      ])
    )
    |> ignore(string(")"))
  end

  def chart_aggregate_group_by do
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

  # ============================================================================
  # Select Combinators
  # ============================================================================

  def select_clause do
    ignore(choice([string("select"), string("s")]))
    |> ignore(ascii_char([?:]))
    |> choice([
      string("*") |> replace("*") |> unwrap_and_tag(:path),
      metadata_field(),
      any_field()
    ])
    |> tag(:select)
  end

  # ============================================================================
  # DateTime Combinators
  # ============================================================================

  def datetime_abbreviations do
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

  def datetime_with_range do
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

  def time_with_range do
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

  def timestamp_value do
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

  # ============================================================================
  # Clause Combinators
  # ============================================================================

  def timestamp_clause do
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

  def metadata_level_clause do
    string("metadata.level")
    |> ignore(string(":"))
    |> concat(range_operator(level_strings()))
    |> tag(:metadata_level_clause)
    |> reduce(:to_rule)
  end

  # ============================================================================
  # Condition functions used by `repeat_while`
  # ============================================================================

  @spec not_quote(binary(), list(), non_neg_integer(), non_neg_integer()) ::
          {:cont, list()} | {:halt, list()}
  def not_quote(<<?\\, ?", _::binary>>, context, _, _), do: {:cont, context}
  def not_quote(<<?", _::binary>>, context, _, _), do: {:halt, context}
  def not_quote(_, context, _, _), do: {:cont, context}

  @spec not_whitespace(binary(), list(), non_neg_integer(), non_neg_integer()) ::
          {:cont, list()} | {:halt, list()}
  def not_whitespace(<<c, _::binary>>, context, _, _)
      when c in [?\s, ?\n, ?\t, ?\v, ?\r, ?\f, ?\b],
      do: {:halt, context}

  def not_whitespace("", context, _, _), do: {:halt, context}
  def not_whitespace(_, context, _, _), do: {:cont, context}

  @spec not_right_paren(binary(), list(), non_neg_integer(), non_neg_integer()) ::
          {:cont, list()} | {:halt, list()}
  def not_right_paren(<<?), _::binary>>, context, _, _), do: {:halt, context}
  def not_right_paren(_, context, _, _), do: {:cont, context}
end
