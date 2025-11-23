defmodule Logflare.Backends.UserMonitoring do
  @moduledoc """
  Routes certain user-specific signals to their own System Sources
  """

  import Telemetry.Metrics
  import Logflare.Utils.Guards
  alias Logflare.Users
  alias Logflare.Sources
  alias Logflare.Logs
  alias Logflare.Logs.Processor
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest

  def get_otel_exporter do
    export_period =
      case Application.get_env(:logflare, :env) do
        :test -> 100
        _ -> 60_000
      end

    otel_exporter_opts =
      [
        metrics: metrics(),
        resource: %{
          name: "Logflare",
          service: %{
            name: "Logflare",
            version: Application.spec(:logflare, :vsn) |> to_string()
          },
          node: inspect(Node.self()),
          cluster: Application.get_env(:logflare, :metadata)[:cluster]
        },
        export_callback: &exporter_callback/2,
        extract_tags: &extract_tags/2,
        name: :user_metrics_exporter,
        otlp_endpoint: "",
        export_period: export_period
      ]

    [{OtelMetricExporter, otel_exporter_opts}]
  end

  def metrics do
    [
      counter("logflare.backends.ingest.ingested_count",
        measurement: :ingested_bytes,
        keep: &keep_metric_function/1,
        description: "Count of events ingested by backend for a source"
      ),
      sum("logflare.backends.ingest.ingested_bytes",
        keep: &keep_metric_function/1,
        description: "Amount of bytes ingested by backend for a source"
      ),
      sum("logflare.backends.ingest.egress.request_bytes",
        keep: &keep_metric_function/1,
        description:
          "Amount of bytes egressed by backend for a source, currently only supports HTTP"
      )
    ]
  end

  def keep_metric_function(metadata) do
    case Users.get_related_user_id(metadata) do
      nil -> false
      user_id -> Users.Cache.get(user_id).system_monitoring
    end
  end

  # take all metadata string keys and non-nested values
  # made public to enable benchmarking
  def extract_tags(_metric, metadata) do
    for {key, value} <- metadata,
        is_binary(key) and !is_list_or_map(value) and value != nil,
        into: %{} do
      {key, value}
    end
  end

  defp exporter_callback({:metrics, metrics}, config) do
    metrics
    |> Enum.group_by(fn metric ->
      metric
      |> Protobuf.encode()
      |> Protobuf.decode(Opentelemetry.Proto.Metrics.V1.Metric)
      |> Map.get(:data)
      |> Logflare.Logs.OtelMetric.handle_metric_data(%{})
      |> hd()
      |> Map.get("attributes")
      |> Users.get_related_user_id()
    end)
    |> Enum.each(fn {user_id, user_metrics} ->
      source =
        Sources.Cache.get_by(user_id: user_id, system_source_type: :metrics)
        |> Sources.Cache.preload_rules()
        |> Sources.refresh_source_metrics()

      user_metrics
      |> OtelMetricExporter.Protocol.build_metric_service_request(config.resource)
      |> Protobuf.encode()
      |> Protobuf.decode(ExportMetricsServiceRequest)
      |> Map.get(:resource_metrics)
      |> Processor.ingest(Logs.OtelMetric, source)
    end)

    :ok
  end

  @doc """
  Intercepts Logger messages related to specific users, and send them to the respective
  System Source when the user has activated it
  """
  def log_interceptor(log_event, _) do
    with %{meta: meta} <- log_event,
         user_id when is_integer(user_id) <- Users.get_related_user_id(meta),
         %{system_monitoring: true} <- Users.Cache.get(user_id),
         %{} = source <- get_logs_system_source(user_id) do
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

  defp get_logs_system_source(user_id),
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
