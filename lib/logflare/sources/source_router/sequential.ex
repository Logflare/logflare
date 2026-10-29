defmodule Logflare.Sources.SourceRouter.Sequential do
  @moduledoc false

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
    Enum.all?(lql_filters, &filter_matches?(le_body, &1))
  end

  @spec filter_matches?(map(), FilterRule.t()) :: boolean()
  defp filter_matches?(le_body, %FilterRule{path: path, modifiers: modifiers} = lql_filter) do
    negate? = modifiers[:negate] == true

    le_body
    |> collect_by_path(path)
    |> any_value_matches?(lql_filter, negate?, nil)
  end

  @spec any_value_matches?(list(), FilterRule.t(), boolean(), (term() -> boolean()) | nil) ::
          boolean()
  defp any_value_matches?([], _lql_filter, _negate?, _matcher), do: false
  defp any_value_matches?([nil | _], _lql_filter, true, _matcher), do: true

  defp any_value_matches?([nil | rest], lql_filter, false, matcher),
    do: any_value_matches?(rest, lql_filter, false, matcher)

  defp any_value_matches?(values, lql_filter, negate?, nil),
    do: any_value_matches?(values, lql_filter, negate?, build_matcher(lql_filter))

  defp any_value_matches?([le_value | rest], lql_filter, negate?, matcher) do
    matcher.(le_value) != negate? or any_value_matches?(rest, lql_filter, negate?, matcher)
  end

  @spec build_matcher(FilterRule.t()) :: (term() -> boolean())
  defp build_matcher(%FilterRule{operator: :range, values: [lvalue, rvalue]}),
    do: &(&1 >= lvalue and &1 <= rvalue)

  defp build_matcher(%FilterRule{operator: operator, value: value})
       when operator in [:list_includes, :=],
       do: &(&1 == value)

  defp build_matcher(%FilterRule{operator: operator, value: value})
       when operator in [:list_includes_regexp, :"~"] do
    regex = ~r/#{value}/u
    &(stringify(&1) =~ regex)
  end

  defp build_matcher(%FilterRule{operator: :string_contains, value: value}) do
    pattern = stringify(value)
    &String.contains?(stringify(&1), pattern)
  end

  defp build_matcher(%FilterRule{operator: :<=, value: value}), do: &(&1 <= value)
  defp build_matcher(%FilterRule{operator: :<, value: value}), do: &(&1 < value)
  defp build_matcher(%FilterRule{operator: :>=, value: value}), do: &(&1 >= value)
  defp build_matcher(%FilterRule{operator: :>, value: value}), do: &(&1 > value)

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
