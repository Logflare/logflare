defmodule Logflare.Logs do
  @moduledoc false
  require Logger
  use Publicist

  alias Logflare.LogEvent, as: LE
  alias Logflare.Logs.{RejectedLogEvents}
  alias Logflare.{SystemMetrics, Source, Sources}
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
    |> Enum.map(&Map.put(&1, :make_from, "ingest"))
    |> Enum.map(&LE.make(&1, %{source: source}))
    |> Enum.map(fn %LE{} = le ->
      if le.valid do
        :ok = SourceRouting.route_to_sinks_and_ingest(le)
        :ok = ingest(le)
        :ok = broadcast(le)
      else
        :ok = RejectedLogEvents.ingest(le)
      end

      le
    end)
    |> Enum.reduce([], fn le, acc ->
      if le.valid do
        acc
      else
        [le.validation_error | acc]
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

    le = apply_custom_event_message(le)

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

  defp apply_custom_event_message(%LE{source: %Source{} = source} = le) do
    message = make_message(le, source)

    Kernel.put_in(le.body.message, message)
  end

  defp make_message(le, source) do
    message = le.body.message

    if source.custom_event_message_keys do
      custom_message_keys =
        source.custom_event_message_keys
        |> String.split(",", trim: true)
        |> Enum.map(&String.trim/1)

      Enum.map(custom_message_keys, fn x ->
        case x do
          "id" ->
            le.id

          "message" ->
            message

          "metadata." <> rest ->
            query_json(le.body.metadata, "$.#{rest}")

          "m." <> rest ->
            query_json(le.body.metadata, "$.#{rest}")
        end
      end)
      |> Enum.join(" | ")
    else
      message
    end
  end

  defp query_json(metadata, query) do
    case Warpath.query(metadata, query) do
      {:ok, v} ->
        inspect(v)

      {:error, _} ->
        "json_path_query_error"
    end
  end
end
