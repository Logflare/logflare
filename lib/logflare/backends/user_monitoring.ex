defmodule Logflare.Backends.UserMonitoring do
  @moduledoc """
  Routes certain user-specific signals to their own System Sources
  """

  import Telemetry.Metrics

  alias Logflare.Users
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Logs
  alias Logflare.Logs.Processor
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest

  def get_otel_exporter(source, user) do
    otel_exporter_opts =
      [
        metrics: metrics(source),
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

  def metrics(reference) do
    keep_function = keep_metric_function(reference)

    [
      sum("logflare.backends.ingest.event_count",
        tags: [:backend_id, :source_id],
        keep: keep_function,
        description: "Count of events ingested by backend for a source"
      )
    ]
  end

  def keep_metric_function(%Source{} = source) do
    fn metadata ->
      case Users.get_related_user_id(metadata) do
        nil -> false
        user_id -> user_id == source.user_ id && user_monitoring?(user_id)
      end
    end
  end

  def keep_metric_function(:main_exporter) do
    fn metadata ->
      case Users.get_related_user_id(metadata) do
        nil -> true
        user_id -> !Users.Cache.get(user_id).system_monitoring
      end
    end
  end

  defp user_monitoring?(user_id),
    do: Users.Cache.get(user_id).system_monitoring

  defp generate_exporter_callback(source) do
    fn {:metrics, metrics}, config ->
      refreshed_source = Sources.refresh_source_metrics(source)

      metrics
      |> OtelMetricExporter.Protocol.build_metric_service_request(config.resource)
      |> Protobuf.encode()
      |> Protobuf.decode(ExportMetricsServiceRequest)
      |> Map.get(:resource_metrics)
      |> Processor.ingest(Logs.OtelMetric, refreshed_source)

      :ok
    end
  end

  @doc """
  Intercepts Logger messages related to specific users, and send them to the respective
  System Source when the user has activated it
  """
  def log_interceptor(log_event, _) do
    with %{meta: meta} <- log_event,
         user_id when is_integer(user_id) <- Users.get_related_user_id(meta),
         %{system_monitoring: true} <- Users.Cache.get(user_id),
         %{} = source <- get_system_source(user_id) do
      LogflareLogger.Formatter.format(
        log_event.level,
        format_message(log_event),
        get_datetime(),
        meta
      )
      |> List.wrap()
      |> Processor.ingest(Logs.Raw, source)

      :stop
    else
      _ -> :ignore
    end
  end

  defp get_system_source(user_id),
    do:
      Sources.Cache.get_by(user_id: user_id, system_source_type: :logs)
      |> Sources.refresh_source_metrics()
      |> Sources.Cache.preload_rules()

  defp format_message(event),
    do:
      :logger_formatter.format(event, %{single_line: true, template: [:msg]})
      |> IO.iodata_to_binary()

  defp get_datetime do
    dt = NaiveDateTime.utc_now()
    {date, {hour, minute, second}} = NaiveDateTime.to_erl(dt)
    {date, {hour, minute, second, dt.microsecond}}
  end
end
