defmodule Logflare.Backends.UserMonitoring do
  @moduledoc """
  Routes certain user-specific signals to their own System Sources
  """

  import Telemetry.Metrics
  alias Logflare.Logs
  alias Logflare.Logs.Processor
  alias Logflare.Sources
  alias Logflare.Users

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
  Intercepts Logger messages related to specific users, and send them to the respective
  System Source when the user has activated it
  """
  def log_interceptor(%{meta: %{system_source: true}}, _), do: :ignore

  def log_interceptor(%{meta: %{user_id: user_id} = meta} = log_event, _)
      when is_integer(user_id) do
    with %{system_monitoring: true} <- Users.Cache.get(user_id),
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
