defmodule Logflare.Lql.Parser.BasicCombinators do
  @moduledoc """
  Basic parser combinators for LQL parsing.
  """

  import NimbleParsec

  @isolated_string :isolated_string
  @list_includes_op :list_includes
  @list_includes_regex_op :list_includes_regexp

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

  def any_field() do
    ascii_string([?a..?z, ?A..?Z, ?., ?_, ?0..?9], min: 1)
    |> reduce({List, :to_string, []})
    |> unwrap_and_tag(:path)
    |> label("schema field")
  end

  def metadata_field() do
    choice([string("metadata"), string("m") |> replace("metadata")])
    |> string(".")
    |> ascii_string([?a..?z, ?A..?Z, ?., ?_, ?0..?9], min: 2)
    |> reduce({List, :to_string, []})
    |> unwrap_and_tag(:path)
    |> label("metadata field")
  end

  def operator() do
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

  def null() do
    string("NULL") |> replace(:NULL)
  end

  def field_value() do
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

  def invalid_match_all_value() do
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

  # Condition functions used by repeat_while
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
