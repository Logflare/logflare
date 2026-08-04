defmodule Logflare.Sources.SourceRouter do
  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.LogEvent, as: LE
  alias Logflare.Rules.Rule
  alias Logflare.Sources
  alias Logflare.Sources.Source

  @default_router Logflare.Sources.SourceRouter.RulesTree

  @doc """
  An algorithm returning Rules that match provided LogEvent
  """
  @callback matching_rules(LE.t(), Source.t()) :: [Rule.t()]

  @spec route_to_sinks_and_ingest(LE.t() | [LE.t()], Source.t(), module()) :: LE.t() | [LE.t()]
  def route_to_sinks_and_ingest(events, source, router \\ @default_router)

  def route_to_sinks_and_ingest(events, source, router) when is_list(events) do
    {events, measurements} =
      Enum.map_reduce(events, {0, 0}, fn event, totals ->
        {event, measurements} = route_event(event, source, router)
        {event, add_measurements(totals, measurements)}
      end)

    emit_rule_match_telemetry(measurements)
    events
  end

  def route_to_sinks_and_ingest(%LE{} = le, source, router) do
    {le, measurements} = route_event(le, source, router)
    emit_rule_match_telemetry(measurements)
    le
  end

  defp route_event(%LE{via_rule_id: id} = le, _source, _router) when id != nil,
    do: {le, {0, 0}}

  defp route_event(%LE{via_rule_id: nil} = le, source, router) do
    started_at = System.monotonic_time()
    rules = router.matching_rules(le, source)
    duration = System.monotonic_time() - started_at

    Enum.each(rules, &do_routing(&1, le, source))

    {le, {duration, 1}}
  end

  defp add_measurements(
         {total_duration, total_event_count},
         {duration, event_count}
       ) do
    {total_duration + duration, total_event_count + event_count}
  end

  defp emit_rule_match_telemetry({_duration, 0}), do: :ok

  defp emit_rule_match_telemetry({duration, event_count}) do
    :telemetry.execute(
      [:logflare, :sources, :rules, :match],
      %{duration: duration, event_count: event_count},
      %{}
    )
  end

  defp do_routing(%Rule{backend_id: backend_id} = rule, %LE{} = le, source)
       when backend_id != nil do
    # route to a backend
    backend = Backends.Cache.get_backend(backend_id)
    le = %{le | via_rule_id: rule.id}
    if SourceSup.rule_child_started?(rule) == false, do: SourceSup.start_rule_child(rule)

    # ingest to a specific backend
    Backends.ingest_logs([le], source, backend)
  end

  defp do_routing(%Rule{sink: sink} = rule, %LE{} = le, _source) when sink != nil do
    sink_source =
      Sources.Cache.get_by(token: rule.sink) |> Sources.refresh_source_metrics_for_ingest()

    le = %{le | source_id: sink_source.id, via_rule_id: rule.id}

    Backends.ensure_source_sup_started(sink_source)
    Backends.ingest_logs([le], sink_source)
  end

  defp do_routing(%Rule{sink: nil}, _le, _source) do
    {:error, :no_sink}
  end
end
