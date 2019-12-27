defmodule Logflare.Logs do
  @moduledoc false
  require Logger
  use Publicist

  alias Logflare.LogEvent, as: LE
  alias Logflare.Logs.{RejectedLogEvents}
  alias Logflare.{SystemMetrics, Source, Sources}
  alias Logflare.Source.{BigQuery.Buffer, RecentLogsServer}
  alias Logflare.Logs.SourceRouting

  @spec ingest_logs(list(map), Source.t()) :: :ok | {:error, term}
  def ingest_logs(log_params_batch, %Source{} = source) do
    log_params_batch
    |> Enum.map(&LE.make(&1, %{source: source}))
    |> Enum.map(fn %LE{} = le ->
      if le.valid? do
        SourceRouting.route_to_sinks_and_ingest(le)
        ingest(le)
        broadcast(le)
      else
        RejectedLogEvents.ingest(le)
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
    source_table_string = Atom.to_string(source.token)
    # indvididual source genservers
    RecentLogsServer.push(source.token, le)
    Buffer.push(source_table_string, le)

    # all sources genservers
    Sources.Counters.incriment(source.token)
    SystemMetrics.AllLogsLogged.incriment(:total_logs_logged)
  end

  def broadcast(%LE{} = le) do
    # broadcasters
    Source.ChannelTopics.broadcast_new(le)
  end
end
