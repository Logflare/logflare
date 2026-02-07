defmodule Logflare.FetchQueries.FetchQueryWorker do
  @moduledoc """
  Oban worker for executing fetch queries and ingesting data into sources.
  """

  use Oban.Worker,
    queue: :fetch,
    max_attempts: 1,
    unique: [
      period: :infinity,
      keys: [:fetch_query_id, :scheduled_at],
      states: [:available, :scheduled, :executing]
    ]

  alias Logflare.Backends
  alias Logflare.FetchQueries
  alias Logflare.FetchQueries.Executor
  alias Logflare.LogEvent

  require Logger

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"fetch_query_id" => id}}) do
    with {:ok, fetch_query} <- load_fetch_query(id),
         {:ok, %{rows: events}} <- Executor.execute(fetch_query),
         {:ok, count} <- ingest_events(events, fetch_query) do
      Logger.info("Fetch query completed",
        fetch_query_id: id,
        count: count
      )

      :telemetry.execute([:logflare, :fetch_query, :success], %{count: count}, %{
        fetch_query_id: id
      })

      :ok
    else
      {:error, :not_found} ->
        Logger.warning("Fetch query not found", fetch_query_id: id)
        :ok

      {:error, reason} = error ->
        Logger.error("Fetch query failed",
          fetch_query_id: id,
          reason: inspect(reason)
        )

        :telemetry.execute([:logflare, :fetch_query, :failure], %{}, %{
          fetch_query_id: id,
          reason: inspect(reason)
        })

        error
    end
  end

  defp load_fetch_query(id) do
    case FetchQueries.get_fetch_query(id) do
      nil -> {:error, :not_found}
      query -> {:ok, FetchQueries.preload_fetch_query(query)}
    end
  end

  defp ingest_events(log_events, fetch_query) do
    case Backends.ingest_logs(log_events, fetch_query.source) do
      {:ok, count} -> {:ok, count}
      error -> error
    end
  end
end
