defmodule Logflare.Table do
  @moduledoc """
  Manages the individual table for the source. Limits things in the table to 1000. Manages TTL for
  things in the table. Handles loading the table from the disk if found on startup.
  """

  use GenServer

  alias Logflare.TableCounter
  alias Logflare.Logs
  alias Logflare.SourceData
  alias Logflare.SourceRateCounter
  alias Logflare.Google.BigQuery
  alias Logflare.TableMailer
  alias Logflare.TableTexter
  alias Logflare.TableBuffer
  alias Logflare.TableBigQueryPipeline
  alias Logflare.TableBigQuerySchema
  alias Logflare.Google.BigQuery.GenUtils

  require Logger

  # one month
  @ttl 2_592_000_000_000
  @ttl_timer 1_000
  @prune_timer 1_000

  def start_link(website_table) do
    GenServer.start_link(__MODULE__, website_table, name: website_table)
  end

  ## Client

  def init(state) do
    check_ttl()
    prune()

    state = [{:source_token, state}]
    {:ok, state, {:continue, :boot}}
  end

  def push(website_table, event) do
    GenServer.cast(website_table, {:push, website_table, event})
  end

  ## Server

  def handle_continue(:boot, state) do
    website_table = state[:source_token]
    bigquery_project_id = GenUtils.get_project_id(website_table)
    bigquery_table_ttl = GenUtils.get_table_ttl(website_table)
    tab_path = "tables/" <> Atom.to_string(website_table) <> ".tab"

    BigQuery.init_table!(website_table, bigquery_project_id, bigquery_table_ttl)

    case :ets.tabfile_info(String.to_charlist(tab_path)) do
      {:ok, _info} ->
        case :ets.file2tab(String.to_charlist(tab_path), verify: true) do
          {:ok, _table} ->
            restore_table(website_table, bigquery_project_id)

          {:error, _reason} ->
            fresh_table(website_table, bigquery_project_id)
        end

      {:error, _reason} ->
        fresh_table(website_table, bigquery_project_id)
    end

    state = state ++ [{:bigquery_project_id, bigquery_project_id}]

    TableMailer.start_link(website_table)
    TableTexter.start_link(website_table)
    TableBuffer.start_link(website_table)
    TableBigQueryPipeline.start_link(state)
    TableBigQuerySchema.start_link(state)

    {:noreply, state}
  end

  def handle_cast({:push, website_table, event}, state) do
    :ets.insert(website_table, event)
    {:noreply, state}
  end

  def handle_info(:ttl, state) do
    website_table = state[:source_token]
    first = :ets.first(website_table)

    case first != :"$end_of_table" do
      true ->
        {timestamp, _unique_int, _monotime} = first
        now = System.os_time(:microsecond)
        day_ago = now - @ttl

        if timestamp < day_ago do
          # :ets.delete_match(website_table) I'm too dumb for this
          # https://github.com/ericmj/ex2ms

          :ets.delete(website_table, first)
          TableCounter.decriment(website_table)

          case :ets.info(LogflareWeb.Endpoint) do
            :undefined ->
              Logger.error("Endpoint not up yet!")

            _ ->
              Logs.broadcast_log_count(website_table)
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
    website_table = state[:source_token]
    {:ok, count} = TableCounter.log_count(website_table)

    case count > 100 do
      true ->
        for _log <- 101..count do
          log = :ets.first(website_table)
          :ets.delete(website_table, log)
          TableCounter.decriment(website_table)
        end

        prune()
        {:noreply, state}

      false ->
        prune()
        {:noreply, state}
    end
  end

  ## Private Functions
  defp restore_table(website_table, bigquery_project_id) do
    Logger.info("ETS loaded table: #{website_table}")
    log_count = SourceData.get_log_count(website_table, bigquery_project_id)
    ets_count = SourceData.get_ets_count(website_table)
    TableCounter.create(website_table)
    TableCounter.incriment_ets_count(website_table, ets_count)
    TableCounter.incriment_total_count(website_table, log_count)
    SourceRateCounter.start_link(website_table, ets_count)
  end

  defp fresh_table(website_table, bigquery_project_id) do
    Logger.info("ETS created table: #{website_table}")
    log_count = SourceData.get_log_count(website_table, bigquery_project_id)
    table_args = [:named_table, :ordered_set, :public]
    :ets.new(website_table, table_args)
    TableCounter.create(website_table)
    TableCounter.incriment_total_count(website_table, log_count)
    SourceRateCounter.start_link(website_table, 0)
  end

  defp check_ttl() do
    Process.send_after(self(), :ttl, @ttl_timer)
  end

  defp prune() do
    Process.send_after(self(), :prune, @prune_timer)
  end
end
