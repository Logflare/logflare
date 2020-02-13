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
        length(rule.lql_filters) >= 1 ->
          if route_with_lql_rules?(le, rule) do
            sink_source = Sources.Cache.get_by(token: rule.sink)
            routed_le = %{le | source: sink_source, via_rule: rule}
            :ok = ingest(routed_le)
            :ok = broadcast(routed_le)
          end

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
    flat_le =
      le.body
      |> Map.from_struct()
      |> Iteraptor.to_flatmap()
      |> MapKeys.to_strings()

    flat_le =
      flat_le
      |> Map.put("event_message", flat_le["message"])

    lql_rules_match? =
      Enum.reduce_while(lql_filters, false, fn lql_filter, _acc ->
        %Lql.FilterRule{
          path: path,
          value: value,
          operator: operator
        } = lql_filter

        le_value = flat_le[path]

        {operator, value} =
          case operator do
            :"~" -> {:=~, ~r/#{value}/u}
            := -> {:==, value}
            :list_includes -> {:in, value}
            op -> {op, value}
          end

        lql_filter_matches? =
          case operator do
            :in ->
              matches_list_include?(flat_le, path, value)

            _ ->
              Map.has_key?(flat_le, path) &&
                apply(Kernel, operator, [
                  le_value,
                  value
                ])
          end

        if lql_filter_matches? do
          {:cont, true}
        else
          {:halt, false}
        end
      end)

    lql_rules_match?
  end

  def matches_list_include?(flat_le, path, value) do
    Enum.reduce_while(flat_le, nil, fn {k, v}, acc ->
      flat_map_path_matches? = String.replace(k, ~r/(.+)\.\d+$/, "\\1") === path

      if flat_map_path_matches? and v === value do
        {:halt, true}
      else
        {:cont, false}
      end
    end)
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
