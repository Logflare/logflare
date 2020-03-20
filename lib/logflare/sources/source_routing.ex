defmodule Logflare.Logs.SourceRouting do
  @moduledoc false
  alias Logflare.{Source, Sources}
  alias Logflare.Rule
  alias Logflare.Lql
  alias Logflare.LogEvent, as: LE
  import Logflare.Logs, only: [ingest: 1, broadcast: 1]
  require Logger

  @spec route_to_sinks_and_ingest(LE.t()) :: :ok | :noop
  def route_to_sinks_and_ingest(%LE{via_rule: %Rule{} = rule} = le) do
    Logger.error(
      "LogEvent #{le.id} has already been routed using the rule #{rule.id}, can't proceed!"
    )

    :noop
  end

  def route_to_sinks_and_ingest(%LE{body: body, source: source, via_rule: nil} = le) do
    %Source{rules: rules} = source

    for rule <- rules do
      cond do
        length(rule.lql_filters) >= 1 && route_with_lql_rules?(le, rule) ->
          sink_source = Sources.Cache.get_by(token: rule.sink)
          routed_le = %{le | source: sink_source, via_rule: rule}
          :ok = ingest(routed_le)
          :ok = broadcast(routed_le)

        rule.regex_struct && Regex.match?(rule.regex_struct, body.message) ->
          route_with_regex(le, rule)

        true ->
          :noop
      end
    end

    :ok
  end

  @spec route_with_lql_rules?(LE.t(), Rule.t()) :: boolean()
  def route_with_lql_rules?(%LE{} = le, %Rule{lql_filters: lql_filters})
      when length(lql_filters) >= 1 do
    le = Map.put(le.params, "event_message", le.params["message"])

    lql_rules_match? =
      Enum.reduce_while(lql_filters, true, fn lql_filter, acc ->
        %Lql.FilterRule{path: path, value: value, operator: operator, modifiers: mds} = lql_filter

        le_values = collect_by_path(le, path)

        lql_filter_matches_any_of_nested_values? =
          Enum.reduce_while(le_values, false, fn le_value, _acc ->
            lql_filter_matches? =
              cond do
                is_nil(le_value) ->
                  false

                operator == :range ->
                  [lvalue, rvalue] = lql_filter.values
                  le_value >= lvalue and le_value <= rvalue

                operator == :list_includes ->
                  apply(Kernel, :==, [le_value, value])

                operator == := ->
                  apply(Kernel, :==, [le_value, value])

                operator == :"~" ->
                  apply(Kernel, :=~, [le_value, ~r/#{value}/u])

                operator in [:<=, :<, :>=, :>] ->
                  apply(Kernel, operator, [le_value, value])
              end

            lql_filter_matches? =
              if mds[:negate] do
                not lql_filter_matches?
              else
                lql_filter_matches?
              end

            if lql_filter_matches? do
              {:halt, lql_filter_matches?}
            else
              {:cont, false}
            end
          end)

        if lql_filter_matches_any_of_nested_values? do
          {:cont, true}
        else
          {:halt, false}
        end
      end)

    lql_rules_match?
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

  def route_with_regex(%LE{} = le, %Rule{} = rule) do
    sink_source = Sources.Cache.get_by(token: rule.sink)

    if sink_source do
      routed_le = %{le | source: sink_source, via_rule: rule}
      :ok = ingest(routed_le)
      :ok = broadcast(routed_le)
    else
      Logger.error("Sink source for UUID #{rule.sink} doesn't exist")
    end
  end
end
