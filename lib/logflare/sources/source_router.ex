defmodule Logflare.Sources.SourceRouter do
  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.LogEvent, as: LE
  alias Logflare.Rules.Rule
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Sources.SourceRouter.Target

  @default_router Logflare.Sources.SourceRouter.RulesTree

  @doc """
  An algorithm returning compact routing targets that match a log event.
  """
  @callback matching_rules(LE.t(), Source.t()) :: [Target.t() | Rule.t()]
  @callback prepare(Source.t()) :: term()
  @callback matching_rules(LE.t(), Source.t(), term()) :: [Target.t()]
  @callback matching_rules_with_state(LE.t(), Source.t(), term()) :: {[Target.t()], term()}

  @optional_callbacks prepare: 1, matching_rules: 3, matching_rules_with_state: 3

  @spec route_to_sinks_and_ingest(LE.t() | [LE.t()], Source.t(), module()) :: LE.t() | [LE.t()]
  def route_to_sinks_and_ingest(events, source, router \\ @default_router)

  def route_to_sinks_and_ingest(events, source, router) when is_list(events) do
    prepared = prepare(router, source, events)

    {events, _prepared} =
      Enum.map_reduce(events, prepared, &route_to_sinks_and_ingest(&1, source, router, &2))

    events
  end

  def route_to_sinks_and_ingest(%LE{} = event, source, router) do
    prepared = prepare(router, source, [event])
    {event, _prepared} = route_to_sinks_and_ingest(event, source, router, prepared)
    event
  end

  defp route_to_sinks_and_ingest(%LE{via_rule_id: id} = le, _source, _router, prepared)
       when id != nil,
       do: {le, prepared}

  defp route_to_sinks_and_ingest(%LE{via_rule_id: nil} = le, source, router, prepared) do
    {targets, prepared} = matching_targets(router, le, source, prepared)

    for target <- targets do
      do_routing(target, le, source)
    end

    {le, prepared}
  end

  defp prepare(router, source, events) do
    if Enum.any?(events, &match?(%LE{via_rule_id: nil}, &1)) and Code.ensure_loaded?(router) and
         function_exported?(router, :prepare, 1) and
         function_exported?(router, :matching_rules, 3) do
      router.prepare(source)
    else
      :unprepared
    end
  end

  defp matching_targets(router, event, source, :unprepared),
    do: {router.matching_rules(event, source), :unprepared}

  defp matching_targets(router, event, source, prepared) do
    if function_exported?(router, :matching_rules_with_state, 3) do
      router.matching_rules_with_state(event, source, prepared)
    else
      {router.matching_rules(event, source, prepared), prepared}
    end
  end

  defp do_routing(%Rule{} = rule, %LE{} = le, source),
    do: rule |> Target.from_rule() |> do_routing(le, source)

  defp do_routing({rule_id, backend_id, _sink}, %LE{} = le, source)
       when backend_id != nil do
    backend = Backends.Cache.get_backend(backend_id)
    le = %{le | via_rule_id: rule_id}

    if SourceSup.backend_child_started?(backend_id, source.id) == false,
      do: SourceSup.start_backend_child_by_id(backend_id, source.id)

    Backends.ingest_logs([le], source, backend)
  end

  defp do_routing({rule_id, nil, sink}, %LE{} = le, _source) when sink != nil do
    sink_source =
      Sources.Cache.get_by(token: sink) |> Sources.refresh_source_metrics_for_ingest()

    le = %{le | source_id: sink_source.id, via_rule_id: rule_id}

    Backends.ensure_source_sup_started(sink_source)
    Backends.ingest_logs([le], sink_source)
  end

  defp do_routing({_rule_id, nil, nil}, _le, _source), do: {:error, :no_sink}
end
