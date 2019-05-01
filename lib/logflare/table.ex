defmodule Logflare.Table do
  @moduledoc """
  Manages the individual table for the source. Limits things in the table to 1000. Manages TTL for
  things in the table. Handles loading the table from the disk if found on startup.
  """

  use GenServer

  alias Logflare.TableCounter
  alias LogflareWeb.LogController
  alias Logflare.SourceData

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

    state = [{:table, state}]
    {:ok, state, {:continue, :boot}}
  end

  ## Server

  def handle_continue(:boot, state) do
    website_table = state[:table]
    tab_path = "tables/" <> Atom.to_string(website_table) <> ".tab"

    Logflare.Google.BigQuery.init_table!(website_table)

    case :ets.tabfile_info(String.to_charlist(tab_path)) do
      {:ok, _info} ->
        case :ets.file2tab(String.to_charlist(tab_path), verify: true) do
          {:ok, _table} ->
            restore_table(website_table)

          {:error, _reason} ->
            fresh_table(website_table)
        end

      {:error, _reason} ->
        fresh_table(website_table)
    end

    Logflare.TableMailer.start_link(website_table)
    Logflare.TableTexter.start_link(website_table)
    Logflare.TableBuffer.start_link(website_table)
    Logflare.TableBigQueryPipeline.start_link(website_table)
    Logflare.TableBigQuerySchema.start_link(website_table)

    {:noreply, state}
  end

  def handle_info(:ttl, state) do
    website_table = state[:table]
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
              LogController.broadcast_log_count(website_table)
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
    website_table = state[:table]
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
  defp restore_table(website_table) do
    Logger.info("Loaded table: #{website_table}")
    log_count = SourceData.get_log_count(website_table)
    ets_count = SourceData.get_ets_count(website_table)
    TableCounter.create(website_table)
    TableCounter.incriment_ets_count(website_table, ets_count)
    TableCounter.incriment_total_count(website_table, log_count)
    Logflare.TableRateCounter.start_link(website_table, ets_count)
  end

  defp fresh_table(website_table) do
    Logger.info("Created table: #{website_table}")
    log_count = SourceData.get_log_count(website_table)
    table_args = [:named_table, :ordered_set, :public]
    :ets.new(website_table, table_args)
    TableCounter.create(website_table)
    TableCounter.incriment_total_count(website_table, log_count)
    Logflare.TableRateCounter.start_link(website_table, 0)
  end

  defp check_ttl() do
    Process.send_after(self(), :ttl, @ttl_timer)
  end

  defp prune() do
    Process.send_after(self(), :prune, @prune_timer)
  end
end
