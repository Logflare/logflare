defmodule Logflare.Logs.SourceRouting do
  @moduledoc false
  alias Logflare.{Source, Sources}
  alias Logflare.Rule
  alias Logflare.Lql
  alias Logflare.LogEvent, as: LE
  import Logflare.Logs, only: [ingest: 1, broadcast: 1]
  require Logger

  @spec route_to_sinks_and_ingest(LE.t()) :: term | :noop
  def route_to_sinks_and_ingest(%LE{via_rule: %Rule{} = rule} = le) when not is_nil(rule) do
    Logger.error(
      "LogEvent #{le.id} has already been routed using the rule #{rule.id}, can't proceed!"
    )

    :noop
  end

  def route_to_sinks_and_ingest(
        %LE{body: %{message: message}, via_rule: nil} = le,
        %Source{rules: rules} = source
      ) do
    for rule <- source.rules do
      cond do
        rule.lql_filters ->
          route_with_lql_rules(le, rules)

        rule.regex_struct && Regex.match?(rule.regex_struct, message) ->
          route_with_regex(le, rule)

        true ->
          :noop
      end
    end
  end

  def route_with_lql_rules(%LE{} = le, %Rule{} = rule) do
    flat_le =
      le
      |> Map.from_struct()
      |> Iteraptor.to_flatmap()

    Enum.reduce_while(rule.lql_filters, false, fn lql_filter, _acc ->
      %Lql.FilterRule{
        path: path,
        value: value,
        operator: operator
      } = lql_filter

      le_value = flat_le[path]

      if path in flat_le && apply(Kernel, operator, [le_value, value]) do
        sink_source = Sources.Cache.get_by(token: rule.sink)
        routed_le = %{le | source: sink_source, via_rule: rule}
        ingest(routed_le)
        broadcast(routed_le)
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
      ingest(routed_le)
      broadcast(routed_le)
    else
      Logger.error("Sink source for UUID #{rule.sink} doesn't exist")
    end
  end
end
