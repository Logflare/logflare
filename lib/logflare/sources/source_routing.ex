defmodule Logflare.Logs.SourceRouting do
  @moduledoc false
  alias Logflare.{Source, Sources}
  alias Logflare.Rule
  alias Logflare.Lql
  alias Logflare.LogEvent, as: LE
  alias Logflare.Logs
  require Logger

  @spec route_to_sinks_and_ingest(LE.t()) :: LE.t()
  def route_to_sinks_and_ingest(%LE{via_rule: %Rule{} = rule} = le) do
    Logger.error(
      "LogEvent #{le.id} has already been routed using the rule #{rule.id}, can't proceed!"
    )

    le
  end

  def route_to_sinks_and_ingest(%LE{body: body, source: source, via_rule: nil} = le) do
    %Source{rules: rules} = source

    for rule <- rules do
      regex_struct =
        rule.regex_struct || if(rule.regex != nil, do: Regex.compile!(rule.regex), else: nil)

      cond do
        not Enum.empty?(rule.lql_filters) and route_with_lql_rules?(le, rule) ->
          do_route(le, rule)

        regex_struct != nil and Regex.match?(regex_struct, body["event_message"]) ->
          do_route(le, rule)

        true ->
          le
      end
    end

    le
  end

  # routes the log event
  defp do_route(le, rule) do
    sink_source =
      Sources.Cache.get_by(token: rule.sink) |> Sources.refresh_source_metrics_for_ingest()

    le
    |> Map.put(:source, sink_source)
    |> Map.put(:via_rule, rule)
    |> LE.apply_custom_event_message()
    |> tap(&Logs.ingest/1)
    |> tap(&Logs.broadcast/1)
  end

  @spec route_with_lql_rules?(LE.t(), Rule.t()) :: boolean()
  def route_with_lql_rules?(%LE{body: le_body}, %Rule{lql_filters: lql_filters})
      when lql_filters != [] do
    lql_rules_match? =
      Enum.reduce_while(lql_filters, true, fn lql_filter, _acc ->
        %Lql.FilterRule{path: path, value: value, operator: operator, modifiers: mds} = lql_filter

        le_values = collect_by_path(le_body, path)

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

                operator == :string_contains ->
                  String.contains?(le_value, value)

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
end
