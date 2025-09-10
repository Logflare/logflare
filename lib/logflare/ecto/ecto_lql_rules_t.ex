defmodule Ecto.LqlRules do
  @moduledoc """
  Custom Ecto type for LQL rule lists that handles legacy format conversion.

  Based on `Ecto.Term` but adds automatic conversion from old LQL rule formats
  to new formats during load.
  """

  @behaviour Ecto.Type

  @type t :: any()

  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule

  @spec type :: :binary
  def type, do: :binary

  def cast(value) do
    {:ok, value}
  end

  @spec load(binary() | nil) :: {:ok, any()} | {:error, ArgumentError.t()}
  def load(nil), do: {:ok, nil}
  def load(""), do: {:ok, ""}

  def load(value) do
    term = :erlang.binary_to_term(value)
    converted_term = convert_legacy_rules(term)
    {:ok, converted_term}
  rescue
    e in ArgumentError -> {:error, e}
  end

  @spec dump(any()) :: {:ok, binary() | nil}
  def dump(nil), do: {:ok, nil}
  def dump(""), do: {:ok, ""}

  def dump(value) do
    {:ok, :erlang.term_to_binary(value)}
  end

  def embed_as(_), do: :self

  def equal?(term1, term2), do: term1 === term2

  defp convert_legacy_rules(rules) when is_list(rules) do
    Enum.map(rules, &convert_legacy_rule/1)
  end

  defp convert_legacy_rules(other), do: other

  defp convert_legacy_rule(rule) do
    case rule do
      %FilterRule{} = rule ->
        rule

      %SelectRule{} = rule ->
        rule

      %ChartRule{} = rule ->
        rule

      %{__struct__: Logflare.Lql.FilterRule} = old_rule ->
        struct(FilterRule, Map.delete(old_rule, :__struct__))

      %{__struct__: Logflare.Lql.ChartRule} = old_rule ->
        struct(ChartRule, Map.delete(old_rule, :__struct__))

      map when is_map(map) ->
        cond do
          Map.has_key?(map, :path) and Map.has_key?(map, :operator) ->
            FilterRule.build(Map.to_list(map))

          Map.has_key?(map, :path) and Map.has_key?(map, :wildcard) ->
            struct(SelectRule, map)

          Map.has_key?(map, :aggregate) and Map.has_key?(map, :period) ->
            struct(ChartRule, map)

          true ->
            map
        end

      other ->
        other
    end
  end
end
