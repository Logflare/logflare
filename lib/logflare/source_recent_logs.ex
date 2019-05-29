defmodule Logflare.SourceRecentLogs do
  @moduledoc """
  Manages the individual table for the source. Limits things in the table to 1000. Manages TTL for
  things in the table. Handles loading the table from the disk if found on startup.
  """

  use GenServer

  alias Logflare.SourceCounter
  alias Logflare.Logs
  alias Logflare.SourceData
  alias Logflare.SourceRateCounter
  alias Logflare.Google.BigQuery
  alias Logflare.SourceMailer
  alias Logflare.SourceTexter
  alias Logflare.SourceBuffer
  alias Logflare.SourceBigQueryPipeline
  alias Logflare.SourceBigQuerySchema
  alias Logflare.Google.BigQuery.GenUtils

  require Logger

  # one month
  @prune_timer 1_000

  def start_link(source_id) when is_atom(source_id) do
    GenServer.start_link(__MODULE__, source_id, name: source_id)
  end

  ## Client

  def init(state) do
    prune()

    state = [{:source_token, state}]
    {:ok, state, {:continue, :boot}}
  end

  def push(source_id, event) do
    GenServer.cast(source_id, {:push, source_id, event})
  end

  ## Server

  def handle_continue(:boot, state) do
    source_id = state[:source_token]
    bigquery_project_id = GenUtils.get_project_id(source_id)
    bigquery_table_ttl = GenUtils.get_table_ttl(source_id)

    BigQuery.init_table!(source_id, bigquery_project_id, bigquery_table_ttl)

    table_args = [:named_table, :ordered_set, :public]
    :ets.new(source_id, table_args)

    state = state ++ [{:bigquery_project_id, bigquery_project_id}]

    children = [
      {SourceRateCounter, source_id},
      {SourceMailer, source_id},
      {SourceTexter, source_id},
      {SourceBuffer, source_id},
      {SourceBigQueryPipeline, state},
      {SourceBigQuerySchema, state}
    ]

    Supervisor.start_link(children, strategy: :one_for_all)

    load_logs_from_bigquery(source_id, bigquery_project_id)
    init_counters(source_id, bigquery_project_id)
    Logger.info("ETS table started: #{source_id}")
    {:noreply, state}
  end

  def handle_cast({:push, source_id, event}, state) do
    :ets.insert(source_id, event)
    {:noreply, state}
  end

  def handle_info(:prune, state) do
    source_id = state[:source_token]
    {:ok, count} = SourceCounter.log_count(source_id)

    case count > 100 do
      true ->
        for _log <- 101..count do
          log = :ets.first(source_id)
          :ets.delete(source_id, log)
          SourceCounter.decriment(source_id)
        end

        prune()
        {:noreply, state}

      false ->
        prune()
        {:noreply, state}
    end
  end

  ## Private Functions
  defp load_logs_from_bigquery(source_id, bigquery_project_id) do
    logs =
      with [] <-
             BigQuery.Query.get_events_for_ets(
               source_id,
               bigquery_project_id,
               get_datetime(),
               true
             ),
           [] <-
             BigQuery.Query.get_events_for_ets(
               source_id,
               bigquery_project_id,
               get_datetime(),
               false
             ),
           [] <-
             BigQuery.Query.get_events_for_ets(
               source_id,
               bigquery_project_id,
               get_datetime(-1),
               false
             ),
           [] <-
             BigQuery.Query.get_events_for_ets(
               source_id,
               bigquery_project_id,
               get_datetime(-2),
               false
             ),
           [] <-
             BigQuery.Query.get_events_for_ets(
               source_id,
               bigquery_project_id,
               get_datetime(-3),
               false
             ),
           [] <-
             BigQuery.Query.get_events_for_ets(
               source_id,
               bigquery_project_id,
               get_datetime(-4),
               false
             ),
           [] <-
             BigQuery.Query.get_events_for_ets(
               source_id,
               bigquery_project_id,
               get_datetime(-5),
               false
             ),
           [] <-
             BigQuery.Query.get_events_for_ets(
               source_id,
               bigquery_project_id,
               get_datetime(-6),
               false
             ) do
        []
      else
        logs -> logs
      end

    Enum.each(logs, fn log ->
      Logs.insert_or_push(source_id, log)
    end)
  end

  defp get_datetime(adjustment \\ 0) do
    datetime = DateTime.utc_now()
    seconds = 86_400 * adjustment
    DateTime.add(datetime, seconds, :second)
  end

  defp init_counters(source_id, bigquery_project_id) when is_atom(source_id) do
    log_count = SourceData.get_log_count(source_id, bigquery_project_id)
    SourceCounter.create(source_id)
    SourceCounter.incriment_ets_count(source_id, 0)
    SourceCounter.incriment_total_count(source_id, log_count)
  end

  defp prune() do
    Process.send_after(self(), :prune, @prune_timer)
  end
end
