defmodule Logflare.Logs.SourceRouting do
  @moduledoc false

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.LogEvent, as: LE
  alias Logflare.Logs
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Rules.Rule
  alias Logflare.Source
  alias Logflare.Sources

  @spec route_to_sinks_and_ingest(LE.t()) :: LE.t()
  def route_to_sinks_and_ingest(events) when is_list(events),
    do: Enum.map(events, &route_to_sinks_and_ingest/1)

  def route_to_sinks_and_ingest(%LE{via_rule: %Rule{}} = le), do: le

  def route_to_sinks_and_ingest(%LE{source: %Source{rules: rules}, via_rule: nil} = le) do
    for %Rule{lql_filters: [_ | _]} = rule <- rules, route_with_lql_rules?(le, rule) do
      do_routing(rule, le)
    end

    le
  end

  defp do_routing(%Rule{backend_id: backend_id} = rule, %LE{source: %Source{} = source} = le)
       when backend_id != nil do
    # route to a backend
    backend = Backends.Cache.get_backend(backend_id)
    le = %{le | via_rule: rule}
    if SourceSup.rule_child_started?(rule) == false, do: SourceSup.start_rule_child(rule)

    # ingest to a specific backend
    Backends.ingest_logs([le], source, backend)
  end

  defp do_routing(%Rule{sink: sink} = rule, %LE{source: source} = le) when sink != nil do
    sink_source =
      Sources.Cache.get_by(token: rule.sink) |> Sources.refresh_source_metrics_for_ingest()

    le = %{le | source: sink_source, via_rule: rule}

    if source.v2_pipeline do
      Backends.ensure_source_sup_started(sink_source)
      Backends.ingest_logs([le], sink_source)
    else
      le
      |> LE.apply_custom_event_message()
      |> tap(&Logs.ingest/1)
      |> tap(&Logs.broadcast/1)
    end
  end

  defp do_routing(%Rule{sink: nil}, _le) do
    {:error, :no_sink}
  end

  @spec route_with_lql_rules?(LE.t(), Rule.t()) :: boolean()
  def route_with_lql_rules?(%LE{body: le_body}, %Rule{lql_filters: lql_filters})
      when lql_filters != [] do
    lql_rules_match? =
      Enum.reduce_while(lql_filters, true, fn lql_filter, _acc ->
        %FilterRule{path: path, value: value, operator: operator, modifiers: mds} = lql_filter

        le_values = collect_by_path(le_body, path)

        lql_filter_matches_any_of_nested_values? =
          Enum.reduce_while(le_values, false, fn le_value, _acc ->
            le_str_value = stringify(le_value)

            lql_filter_matches? =
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

  defp stringify(v) when is_integer(v) do
    Integer.to_string(v)
  end

  defp stringify(v) when is_float(v) do
    Float.to_string(v)
  end

  defp stringify(v) when is_binary(v), do: v
  defp stringify(v), do: inspect(v)
end
