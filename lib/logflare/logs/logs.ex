defmodule Logflare.Logs do
  @moduledoc false
  require Logger
  use Publicist
  use Logflare.Commons
  alias Logflare.Logs.{RejectedLogEvents}
  alias Logflare.Source.{BigQuery.Buffer, RecentLogsServer}
  alias Logflare.Logs.SourceRouting
  alias Logflare.Logs.IngestTypecasting
  alias Logflare.Logs.IngestTransformers
  alias Logflare.Source.Supervisor

  @spec ingest_logs(list(map), Source.t()) :: :ok | {:error, term}
  def ingest_logs(log_params_batch, %Source{rules: rules} = source) when is_list(rules) do
    log_params_batch
    |> Enum.map(&IngestTypecasting.maybe_apply_transform_directives/1)
    |> Enum.map(&IngestTransformers.transform(&1, :to_bigquery_column_spec))
    |> Enum.map(&maybe_customize_event_message(&1, source))
    |> Enum.map(&LE.make(&1, %{source: source}))
    |> Enum.map(fn %LE{} = le ->
      if le.valid? do
        :ok = SourceRouting.route_to_sinks_and_ingest(le)
        :ok = ingest(le)
        :ok = broadcast(le)
      else
        :ok = RejectedLogEvents.ingest(le)
      end

      le
    end)
    |> Enum.reduce([], fn log, acc ->
      if log.valid? do
        acc
      else
        [log.validation_error | acc]
      end
    end)
    |> case do
      [] -> :ok
      errors when is_list(errors) -> {:error, errors}
    end
  end

  def ingest(%LE{source: %Source{} = source} = le) do
    # indvididual source genservers
    {:ok, _} = Supervisor.ensure_started(source.token)

    :ok = RecentLogsServer.push(le)
    :ok = Buffer.push(le)

    # all sources genservers
    {:ok, _} = Sources.Counters.incriment(source.token)
    {:ok, :total_logs_logged} = SystemMetrics.AllLogsLogged.incriment(:total_logs_logged)

    :ok
  end

  def broadcast(%LE{} = le) do
    # broadcasters
    :ok = Source.ChannelTopics.broadcast_new(le)

    :ok
  end

  defp maybe_customize_event_message(event, source) do
    if source.custom_event_message_keys do
      custom_message_keys = source.custom_event_message_keys |> String.split(",", trim: true)

      custom_message =
        Enum.map(custom_message_keys, fn x ->
          path = x |> String.trim() |> String.split(".")
          value = get_in(event, path)

          inspect(value)
        end)
        |> Enum.join(" | ")

      Map.put(event, "custom_message", custom_message)
    else
      event
    end
  end
end
