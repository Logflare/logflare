defmodule Logflare.Backends.UserMonitoring do
  @moduledoc """
  Routes certain user-specific signals to their own System Sources
  """

  import Telemetry.Metrics

  alias Logflare.Users
  alias Logflare.Sources
  alias Logflare.Logs
  alias Logflare.Logs.Processor
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest

  def get_otel_exporter(source, user) do
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

  defp system_metrics(source) do
    keep_function = keep_metric_function(source)

    if Application.get_env(:logflare, :env) == :test do
      [
        last_value("logflare.test.user_specific.value",
          description: "To test how user specific metrics are handled by exporter",
          tags: [:backend_id],
          keep: keep_function
        )
      ]
    else
      []
    end
  end

  defp keep_metric_function(source) do
    fn metadata ->
      case Users.get_related_user_id(metadata) do
        nil -> false
        user_id -> user_id == source.user_id
      end
    end
  end

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
