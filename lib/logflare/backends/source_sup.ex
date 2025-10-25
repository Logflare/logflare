defmodule Logflare.Backends.SourceSup do
  @moduledoc false
  use Supervisor

  import Telemetry.Metrics

  alias Logflare.Backends.Backend
  alias Logflare.Backends.SourceSupWorker
  alias Logflare.Backends
  alias Logflare.Sources.Source
  alias Logflare.Users
  alias Logflare.Billing
  alias Logflare.GenSingleton
  alias Logflare.Sources.Source.RateCounterServer
  alias Logflare.Sources.Source.EmailNotificationServer
  alias Logflare.Sources.Source.TextNotificationServer
  alias Logflare.Sources.Source.WebhookNotificationServer
  alias Logflare.Sources.Source.SlackHookServer
  alias Logflare.Sources.Source.BillingWriter
  alias Logflare.Backends.RecentEventsTouch
  alias Logflare.Backends.RecentInsertsBroadcaster
  alias Logflare.Rules.Rule
  alias Logflare.Sources
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.Logs
  alias Logflare.Logs.Processor
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest

  def start_link(%Source{} = source) do
    Supervisor.start_link(__MODULE__, source, name: Backends.via_source(source, __MODULE__))
  end

  def init(source) do
    ingest_backends = Backends.Cache.list_backends(source_id: source.id)

    rules_backends =
      Backends.Cache.list_backends(rules_source_id: source.id)
      |> Enum.map(&%{&1 | register_for_ingest: false})

    user = Users.Cache.get(source.user_id)

    plan = Billing.Cache.get_plan_by_user(user)

    default_backend = Backends.get_default_backend(user)

    specs =
      [default_backend | ingest_backends]
      |> Enum.concat(rules_backends)
      |> Enum.map(&Backend.child_spec(source, &1))
      |> Enum.uniq()

    otel_exporter = maybe_get_otel_exporter(source, user)

    children =
      [
        {RateCounterServer, [source: source]},
        {GenSingleton, child_spec: {RecentEventsTouch, source: source}},
        {RecentInsertsBroadcaster, [source: source]},
        {EmailNotificationServer, [source: source]},
        {TextNotificationServer, [source: source, plan: plan]},
        {WebhookNotificationServer, [source: source]},
        {SlackHookServer, [source: source]},
        {BillingWriter, [source: source]},
        {SourceSupWorker, [source: source]}
      ] ++ otel_exporter ++ specs

    Supervisor.init(children, strategy: :one_for_one)
  end

  #TODO: correctly start system sources on system start and
  defp maybe_get_otel_exporter(%{system_source: true} = source, user) do
    otel_exporter_opts =
      [
        metrics: system_metrics(source),
        resource: %{
          name: "Logflare",
          service: %{
            name: "Logflare",
            version: Application.spec(:logflare, :vsn) |> to_string()
          },
          node: inspect(Node.self()),
          cluster: Application.get_env(:logflare, :metadata)[:cluster]
        },
        export_callback: generate_exporter_callback(source),
        name: :"#{source.name}-#{user.id}",
        otlp_endpoint: ""
      ]

    [{OtelMetricExporter, otel_exporter_opts}]
  end

  defp maybe_get_otel_exporter(_, _),
    do: []

  defp system_metrics(source) do
    keeping_function = metric_keeping_function(source)

    [
      last_value("logflare.sources.test",
        tags: [:source_id],
        keep: keeping_function
      )
    ]
  end

  defp metric_keeping_function(source) do
    fn metadata ->
      case get_entity_from_metadata(metadata) do
        %{user_id: user_id} -> user_id == source.user_id
        _ -> false
      end
    end
  end

  defp get_entity_from_metadata(%{source_id: source_id}),
    do: Sources.Cache.get_by_id(source_id)

  defp get_entity_from_metadata(%{source_token: token}),
    do: Sources.Cache.get_source_by_token(token)

  defp get_entity_from_metadata(%{backend_id: backend_id}),
    do: Backends.Cache.get_backend(backend_id)

  defp get_entity_from_metadata(_), do: nil

  defp generate_exporter_callback(source) do
    fn {:metrics, metrics}, _ ->
      refreshed_source = Sources.refresh_source_metrics(source)

      metrics
      |> OtelMetricExporter.Protocol.build_metric_service_request()
      |> Protobuf.encode()
      |> Protobuf.decode(ExportMetricsServiceRequest)
      |> Map.get(:resource_metrics)
      |> Processor.ingest(Logs.OtelMetric, refreshed_source)

      :ok
    end
  end

  @doc """
  Checks if a rule child is started for a given source/rule.
  Must be a backend rule.
  """
  @spec rule_child_started?(Rule.t()) :: boolean()
  def rule_child_started?(%Rule{backend_id: backend_id, source_id: source_id}) do
    via = Backends.via_source(source_id, AdaptorSupervisor, backend_id)

    if GenServer.whereis(via) do
      true
    else
      false
    end
  end

  @doc """
  Starts a given backend child spec for the backend associated with a rule.
  This backend will not be registered for ingest dispatching.

  This allows for zero-downtime ingestion, as we don't restart the SourceSup supervision tree.
  """
  @spec start_rule_child(Rule.t()) :: Supervisor.on_start_child()
  def start_rule_child(%Rule{backend_id: backend_id} = rule) do
    backend = Backends.Cache.get_backend(backend_id) |> Map.put(:register_for_ingest, false)
    source = Sources.Cache.get_by_id(rule.source_id)
    start_backend_child(source, backend)
  end

  @doc """
  Starts a given backend-souce combination when SourceSup is already running.
  This allows for zero-downtime ingestion, as we don't restart the SourceSup supervision tree.
  """
  @spec start_backend_child(Source.t(), Backend.t()) :: Supervisor.on_start_child()
  def start_backend_child(%Source{} = source, %Backend{} = backend) do
    via = Backends.via_source(source, __MODULE__)
    source = Sources.Cache.get_by_id(source.id)
    spec = Backend.child_spec(source, backend)
    Supervisor.start_child(via, spec)
  end

  @doc """
  Stops a given backend child on SourceSup that is associated with the given Rule.
  """
  @spec stop_rule_child(Rule.t()) :: :ok | {:error, :not_found}
  def stop_rule_child(%Rule{backend_id: backend_id} = rule) do
    backend = Backends.Cache.get_backend(backend_id) |> Map.put(:register_for_ingest, false)
    source = Sources.Cache.get_by_id(rule.source_id)
    stop_backend_child(source, backend)
  end

  @doc """
  Stops a backend child based on a provide source-backend combination.
  """
  @spec stop_backend_child(Source.t(), Backend.t()) :: :ok | {:error, :not_found}
  def stop_backend_child(%Source{} = source, %Backend{id: id}), do: stop_backend_child(source, id)

  def stop_backend_child(%Source{} = source, backend_id) when backend_id != nil do
    via = Backends.via_source(source, __MODULE__)
    # spec = Backend.child_spec(source, backend)

    found_id =
      Supervisor.which_children(via)
      |> Enum.find_value(
        fn {{_mod, _source_id, bid}, _pid, _type, _sup} ->
          bid == backend_id
        end,
        &elem(&1, 0)
      )

    if found_id do
      Supervisor.terminate_child(via, found_id)
      Supervisor.delete_child(via, found_id)
      :ok
    else
      {:error, :not_found}
    end
  end
end
