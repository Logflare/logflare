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
    events
    |> Enum.flat_map(&matching_routes(&1, source, router))
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.each(fn {rule, les} ->
      rule
      |> do_routing(les, source)
      |> maybe_emit_dropped(rule, source, length(les))
    end)

    events
  end

  def route_to_sinks_and_ingest(%LE{} = le, source, router) do
    route_to_sinks_and_ingest([le], source, router)
    le
  end

  @spec matching_routes(LE.t(), Source.t(), module()) :: [{Rule.t(), LE.t()}]
  defp matching_routes(%LE{via_rule_id: id}, _source, _router) when id != nil, do: []

  defp matching_routes(%LE{} = le, source, router) do
    for %Rule{} = rule <- router.matching_rules(le, source), do: {rule, le}
  end

  @spec maybe_emit_dropped(term(), Rule.t(), Source.t(), pos_integer()) :: :ok
  defp maybe_emit_dropped({:error, reason}, rule, source, count)
       when reason in [:source_unavailable, :source_not_found] do
    :telemetry.execute(
      [:logflare, :sources, :source_router, :dropped],
      %{count: count},
      %{reason: reason, source_id: source.id, rule_id: rule.id}
    )
  end

  defp maybe_emit_dropped(_result, _rule, _source, _count), do: :ok

  defp do_routing(%Rule{backend_id: backend_id} = rule, les, source)
       when backend_id != nil do
    # route to a backend
    backend = Backends.Cache.get_backend(backend_id)
    les = Enum.map(les, &%{&1 | via_rule_id: rule.id})
    if SourceSup.rule_child_started?(rule) == false, do: SourceSup.start_rule_child(rule)

    # ingest to a specific backend
    Backends.ingest_logs(les, source, backend)
  end

  defp do_routing(%Rule{sink: sink} = rule, les, _source) when sink != nil do
    Sources.Cache.get_by(token: rule.sink)
    |> Sources.refresh_source_metrics_for_ingest()
    |> case do
      %Source{} = sink_source ->
        les = Enum.map(les, &%{&1 | source_id: sink_source.id, via_rule_id: rule.id})

        Backends.ingest_logs(les, sink_source)

      nil ->
        {:error, :sink_not_found}
    end
  end

  defp do_routing(%Rule{sink: nil}, _les, _source) do
    {:error, :no_sink}
  end
end
