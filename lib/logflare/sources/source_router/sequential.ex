defmodule Logflare.Sources.SourceRouter.Sequential do
  @moduledoc false

  require Logger

  alias Logflare.LogEvent, as: LE
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Rules
  alias Logflare.Rules.Rule

  @behaviour Logflare.Sources.SourceRouter

  @impl true
  def matching_rules(le, source) do
    rules = Rules.Cache.list_rules(source)

    for %Rule{lql_filters: [_ | _]} = rule <- rules, route_with_lql_rules?(le, rule) do
      rule
    end
  end

  @spec route_with_lql_rules?(LE.t(), Rule.t()) :: boolean()
  def route_with_lql_rules?(%LE{body: le_body}, %Rule{lql_filters: lql_filters})
      when lql_filters != [] do
    Enum.all?(lql_filters, fn lql_filter ->
      le_body
      |> collect_by_path(lql_filter.path)
      |> Enum.any?(fn le_value ->
        evaluate_filter_condition(lql_filter, le_value)
      end)
    end)
  end

  defp evaluate_filter_condition(lql_filter, le_value) do
    %FilterRule{value: value, operator: operator, modifiers: modifiers} = lql_filter
    le_str_value = stringify(le_value)

    matches? =
      cond do
        is_nil(le_value) ->
          false

        operator == :range ->
          [lvalue, rvalue] = lql_filter.values
          le_value >= lvalue and le_value <= rvalue

        operator == :list_includes ->
          le_value == value

        operator == :list_includes_regexp ->
          le_str_value =~ ~r/#{value}/u

        operator == :string_contains ->
          String.contains?(le_str_value, stringify(value))

        operator == := ->
          le_value == value

        operator == :"~" ->
          le_str_value =~ ~r/#{value}/u

        operator in [:<=, :<, :>=, :>] ->
          apply(Kernel, operator, [le_value, value])
      end

    if modifiers[:negate], do: not matches?, else: matches?
  end

  defp collect_by_path(params, path) when is_binary(path) do
    collect_by_path(params, String.split(path, "."))
  end

  defp collect_by_path(params, [field]) do
    params
    |> Map.get(field)
    |> List.wrap()
  end

  defp collect_by_path(params, [field | rest]) do
    values =
      case Map.get(params, field) do
        [x | _] = xs when is_map(x) ->
          xs
          |> Enum.map(fn
            x when is_map(x) -> collect_by_path(x, rest)
            _ -> []
          end)
          |> List.flatten()

        [_ | _] = xs ->
          xs

        [] ->
          []

        x when is_map(x) ->
          collect_by_path(x, rest)

        x ->
          x
      end

    List.wrap(values)
  end

  defp stringify(v) when is_integer(v) do
    Integer.to_string(v)
  end

  defp stringify(v) when is_float(v) do
    Float.to_string(v)
  end

  defp stringify(v) when is_binary(v), do: v
  defp stringify(v), do: inspect(v)
end
