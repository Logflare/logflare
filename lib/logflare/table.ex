defmodule Logflare.Table do
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
  @ttl 2_592_000_000_000
  @ttl_timer 1_000
  @prune_timer 1_000

  def start_link(source_id) when is_atom(source_id) do
    GenServer.start_link(__MODULE__, source_id, name: source_id)
  end

  ## Client

  def init(state) do
    check_ttl()
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
    tab_path = "tables/" <> Atom.to_string(source_id) <> ".tab"

    BigQuery.init_table!(source_id, bigquery_project_id, bigquery_table_ttl)

    case :ets.tabfile_info(String.to_charlist(tab_path)) do
      {:ok, _info} ->
        case :ets.file2tab(String.to_charlist(tab_path), verify: true) do
          {:ok, _table} ->
            restore_table(source_id, bigquery_project_id)

          {:error, _reason} ->
            fresh_table(source_id, bigquery_project_id)
        end

      {:error, _reason} ->
        fresh_table(source_id, bigquery_project_id)
    end

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

    {:noreply, state}
  end

  def handle_cast({:push, source_id, event}, state) do
    :ets.insert(source_id, event)
    {:noreply, state}
  end

  def handle_info(:ttl, state) do
    source_id = state[:source_token]
    first = :ets.first(source_id)

    case first != :"$end_of_table" do
      true ->
        {timestamp, _unique_int, _monotime} = first
        now = System.os_time(:microsecond)
        day_ago = now - @ttl

        if timestamp < day_ago do
          # :ets.delete_match(source_id) I'm too dumb for this
          # https://github.com/ericmj/ex2ms

          :ets.delete(source_id, first)
          SourceCounter.decriment(source_id)

          case :ets.info(LogflareWeb.Endpoint) do
            :undefined ->
              Logger.error("Endpoint not up yet!")

            _ ->
              Logs.broadcast_log_count(source_id)
          end
        end

        check_ttl()
        {:noreply, state}

      false ->
        check_ttl()
        {:noreply, state}
    end
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
  defp restore_table(source_id, bigquery_project_id) when is_atom(source_id) do
    Logger.info("ETS loaded table: #{source_id}")
    log_count = SourceData.get_log_count(source_id, bigquery_project_id)
    SourceCounter.create(source_id)
    SourceCounter.incriment_ets_count(source_id, 0)
    SourceCounter.incriment_total_count(source_id, log_count)
  end

  defp fresh_table(source_id, bigquery_project_id) when is_atom(source_id) do
    Logger.info("ETS created table: #{source_id}")
    log_count = SourceData.get_log_count(source_id, bigquery_project_id)
    table_args = [:named_table, :ordered_set, :public]
    :ets.new(source_id, table_args)
    SourceCounter.create(source_id)
    SourceCounter.incriment_total_count(source_id, log_count)
  end

  defp check_ttl() do
    Process.send_after(self(), :ttl, @ttl_timer)
  end

  defp prune() do
    Process.send_after(self(), :prune, @prune_timer)
  end
end
