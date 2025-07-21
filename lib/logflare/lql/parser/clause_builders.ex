defmodule Logflare.Lql.Parser.ClauseBuilders do
  @moduledoc """
  Higher-level clause builders that combine other combinators
  """

  import NimbleParsec

  alias Logflare.Lql.Parser.BasicCombinators
  alias Logflare.Lql.Parser.DateTimeParsers

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

  def metadata_level_clause() do
    string("metadata.level")
    |> ignore(string(":"))
    |> concat(range_operator(level_strings()))
    |> tag(:metadata_level_clause)
    |> reduce(:to_rule)
  end

  defdelegate operator(), to: BasicCombinators
  defdelegate timestamp_value(), to: DateTimeParsers
  defdelegate metadata_field(), to: BasicCombinators
  defdelegate field_value(), to: BasicCombinators
  defdelegate any_field(), to: BasicCombinators
  defdelegate range_operator(combinator), to: BasicCombinators
  defdelegate level_strings(), to: BasicCombinators
end
