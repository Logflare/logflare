defmodule Logflare.Backends.UserMonitoring do
  @moduledoc """
  Routes certain user-specific signals to their own System Sources
  """

  import Telemetry.Metrics

  alias Logflare.Backends
  alias Logflare.Backends.UserMonitoring.IngestPipeline
  alias Logflare.Backends.UserMonitoring.SystemSourceStarter
  alias Logflare.Logs
  alias Logflare.Logs.Processor
  alias Logflare.Sources
  alias Logflare.Users

  @store_name :user_metrics_store
  @delivering_key {__MODULE__, :delivering}

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

  def get_otel_exporter do
    env = Application.get_env(:logflare, :env)

    {pull_interval, export_period} =
      case env do
        :test -> {100, 100}
        _ -> {10_000, :timer.minutes(8) + :rand.uniform(60_000 * 2)}
      end

    exporter_spec =
      {OtelMetricExporter,
       [
         name: @store_name,
         metrics: metrics(),
         pull_mode: true,
         export_period: export_period,
         extract_tags: &__MODULE__.extract_tags/2,
         hibernate_after: 5_000,
         spawn_opt: [fullsweep_after: 10_000]
       ]}

    pipeline_spec =
      {IngestPipeline,
       [
         metric_store_name: @store_name,
         pull_interval: pull_interval,
         batch_size: 500
       ]}

    [exporter_spec, pipeline_spec]
  end

  def keep_metric_function(%{"system_source" => true}), do: false

  def keep_metric_function(metadata) do
    case Users.get_related_user_id(metadata) do
      nil -> false
      user_id -> Users.Cache.get(user_id).system_monitoring
    end
  end

  # take all metadata string keys and non-nested values
  def extract_tags(_metric, metadata) when is_map(metadata) do
    for {key, value}
        when is_binary(key) and not is_nil(value) and not is_list(value) and not is_map(value) <-
          metadata,
        into: %{} do
      {key, value}
    end
  end

  @doc """
  Sends a Logger message for a user to the system logs source of that user.

  The filter acts only when the user has turned on system monitoring. It copies internal Logflare
  log lines, such as adaptor errors, as async system log events. It never sees logs that users
  ingest.

  The filter runs in the process that logs. That process can be inside a `SourceSup` start. A
  start from here can wait on the `SourcesSup` partition that starts the logging process. That
  wait deadlocks the partition. Thus the filter never starts the system logs source itself.

  When the `SourceSup` of the system logs source is up, the filter sends the event to that source.
  When it is down, the filter drops the system log event and asks `SystemSourceStarter` to start
  the `SourceSup` in a different process. The original log line is not affected: it still reaches
  the Logflare logs. Each drop emits `[:logflare, :user_monitoring, :system_logs, :dropped]`.

  Two waits remain in the logging process. The timeout of `Logflare.Backends.start_source_sup/1`
  limits each of them:

  - A rule on the system logs source routes an event to a sink source whose `SourceSup` is down.
  - The `SourceSup` of the system logs source stops between the check and the ingest. In that
    case, the filter drops the system log event after the wait.

  A log line that the ingest emits in the same process does not go through the filter again. The
  filter ignores it, so a failed ingest can not log, intercept and ingest without end.
  """
  @spec log_interceptor(:logger.log_event(), term()) :: :ignore
  def log_interceptor(%{meta: %{system_source: true}}, _), do: :ignore

  def log_interceptor(%{meta: %{user_id: user_id} = meta} = log_event, _)
      when is_integer(user_id) do
    with nil <- Process.get(@delivering_key),
         %{system_monitoring: true} <- Users.Cache.get(user_id),
         %Sources.Source{} = source <- get_system_source_logs(user_id) do
      events =
        log_event.level
        |> LogflareLogger.Formatter.format(format_message(log_event), get_datetime(), meta)
        |> List.wrap()

      deliver(source, events)
      :ignore
    else
      _ -> :ignore
    end
  rescue
    _error ->
      :ignore
  end

  def log_interceptor(_, _), do: :ignore

  @spec deliver(Sources.Source.t(), [map()]) :: :ok
  defp deliver(source, events) do
    Process.put(@delivering_key, true)

    with true <- Backends.source_sup_started?(source),
         {:error, reason} when reason in [:source_unavailable, :source_not_found] <-
           Processor.ingest(events, Logs.Raw, source) do
      drop(source, events, reason)
    else
      false -> drop(source, events, :source_not_started)
      _result -> :ok
    end
  after
    Process.delete(@delivering_key)
  end

  @spec drop(
          Sources.Source.t(),
          [map()],
          :source_not_started | :source_unavailable | :source_not_found
        ) :: :ok
  defp drop(source, events, reason) do
    :telemetry.execute(
      [:logflare, :user_monitoring, :system_logs, :dropped],
      %{count: length(events)},
      %{source_id: source.id, reason: reason}
    )

    SystemSourceStarter.request_start(source.id)
  end

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
