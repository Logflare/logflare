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

  def route_to_sinks_and_ingest(events, source, router) when is_list(events),
    do: Enum.map(events, &route_to_sinks_and_ingest(&1, source, router))

  def route_to_sinks_and_ingest(%LE{via_rule_id: id} = le, _source, _router) when id != nil,
    do: le

  def route_to_sinks_and_ingest(%LE{via_rule_id: nil} = le, source, router) do
    for rule <- router.matching_rules(le, source) do
      do_routing(rule, le, source)
    end

    le
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
