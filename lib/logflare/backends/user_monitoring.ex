defmodule Logflare.Backends.UserMonitoring do
  @moduledoc """
  Routes certain user-specific signals to their own System Sources
  """

  import Telemetry.Metrics
  alias Logflare.Logs
  alias Logflare.Logs.OtelMetric
  alias Logflare.Logs.Processor
  alias Logflare.Sources
  alias Logflare.Users

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
        export_period: export_period,
        max_concurrency: System.schedulers_online(),
        max_batch_size: 2500
      ]

    [{OtelMetricExporter, otel_exporter_opts}]
  end

  def metrics do
    [
      sum("logflare.backends.ingest.ingested_bytes",
        keep: &keep_metric_function/1,
        description: "Amount of bytes ingested by backend for a source"
      ),
      sum("logflare.endpoints.query.total_bytes_processed",
        keep: &keep_metric_function/1,
        description: "Amount of bytes processed by a Logflare Endpoint"
      ),
      counter("logflare.backends.ingest.ingested_count",
        measurement: :ingested_bytes,
        keep: &keep_metric_function/1,
        description: "Count of events ingested by backend for a source"
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

  @doc false
  # take all metadata string keys and non-nested values
  def extract_tags(_metric, metadata) when is_map(metadata) do
    for {key, value}
        when is_binary(key) and not is_nil(value) and not is_list(value) and not is_map(value) <-
          metadata,
        into: %{} do
      {key, value}
    end
  end

  # @doc false
  def exporter_callback({:metrics, metrics}, config, opts \\ []) do
    if Keyword.get(opts, :flow, true) do
      metrics
      |> Stream.flat_map(fn metric ->
        OtelMetric.handle_metric(metric, config.resource, %{})
      end)
      |> Flow.from_enumerable(max_demand: 500, stages: System.schedulers_online())
      |> Flow.map(fn event ->
        {get_in(event, ["attributes", "user_id"]), event}
      end)
      |> Flow.reject(fn {user_id, _} -> is_nil(user_id) end)
      |> Flow.group_by_key()
      |> Flow.stream()
      |> Stream.each(&ingest_grouped_metrics/1)
      |> Stream.run()
    else
      metrics
      |> Enum.reduce(%{}, &metric_reducer(&1, &2, config.resource))
      |> ingest_grouped_metrics()
    end

    :ok
  end

  defp metric_reducer(metric, acc, resource) do
    for event <- OtelMetric.handle_metric(metric, resource, %{}), reduce: acc do
      acc ->
        user_id = Users.get_related_user_id(Map.get(event, "attributes"))
        Map.update(acc, user_id, [event], &[event | &1])
    end
  end

  defp ingest_grouped_metrics(grouped_events)
       when is_list(grouped_events) or is_map(grouped_events) do
    Enum.each(grouped_events, fn {user_id, user_events} ->
      ingest_grouped_metrics({user_id, user_events})
    end)
  end

  defp ingest_grouped_metrics({user_id, user_events}) do
    with %Sources.Source{} = source <-
           Sources.Cache.get_by(user_id: user_id, system_source_type: :metrics) do
      Processor.ingest(user_events, Logs.Raw, source)
    end
  end

  @doc """
  Intercepts Logger messages related to specific users, and send them to the respective
  System Source when the user has activated it
  """
  def log_interceptor(%{meta: meta} = log_event, _) do
    with user_id when is_integer(user_id) <- Users.get_related_user_id(meta),
         %{system_monitoring: true} <- Users.Cache.get(user_id),
         %Sources.Source{} = source <- get_system_source_logs(user_id) do
      log_event.level
      |> LogflareLogger.Formatter.format(format_message(log_event), get_datetime(), meta)
      |> List.wrap()
      |> Processor.ingest(Logs.Raw, source)

      # do not block the event from being shipped.
      :ignore
    else
      _ -> :ignore
    end
  rescue
    _error ->
      :ignore
  end

  def log_interceptor(_, _), do: :ignore

  defp get_system_source_logs(user_id) do
    Sources.Cache.get_by_and_preload_rules(user_id: user_id, system_source_type: :logs)
    |> Sources.refresh_source_metrics_for_ingest()
  end

  defp format_message(%{msg: {:string, msg}}), do: msg
  defp format_message(%{msg: {:report, report}}), do: inspect(report)

  defp format_message(%{msg: {format, args}}) when is_list(args),
    do: :io_lib.format(format, args) |> IO.iodata_to_binary()

  defp format_message(event) do
    event
    |> :logger_formatter.format(%{single_line: true, template: [:msg]})
    |> IO.iodata_to_binary()
  end

  defp get_datetime do
    us = System.system_time(:microsecond)
    {date, {h, m, s}} = :calendar.system_time_to_universal_time(div(us, 1_000_000), :second)
    {date, {h, m, s, {rem(us, 1_000_000), 6}}}
  end
end
