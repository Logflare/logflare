defmodule Logflare.Source.RecentLogsServer do
  @moduledoc """
  Manages the individual table for the source. Limits things in the table to 1000. Manages TTL for
  things in the table. Handles loading the table from the disk if found on startup.
  """

  use GenServer

  alias Logflare.Sources.Counters
  alias Logflare.Google.{BigQuery, BigQuery.GenUtils}
  alias Number.Delimit
  alias Logflare.Source.BigQuery.{Schema, Pipeline, Buffer}
  alias Logflare.Source.{Data, EmailNotificationServer, TextNoticationServer, RateCounterServer}
  alias Logflare.LogEvent, as: LE
  alias Logflare.Sources

  require Logger

  # one month
  @prune_timer 1_000

  def start_link(source_id) when is_atom(source_id) do
    GenServer.start_link(__MODULE__, source_id, name: source_id)
  end

  ## Client

  def init(state) do
    Process.flag(:trap_exit, true)
    prune()

    state = [{:source_token, state}]
    {:ok, state, {:continue, :boot}}
  end

  def push(source_id, %LE{} = log_event) do
    GenServer.cast(source_id, {:push, source_id, log_event})
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

    # TODO: Should really be starting these all up with the same struct because some need the same data
    children = [
      {RateCounterServer, source_id},
      {EmailNotificationServer, source_id},
      {TextNoticationServer, source_id},
      {Buffer, source_id},
      {Pipeline, state},
      {Schema, state}
    ]

    Supervisor.start_link(children, strategy: :one_for_all)

    Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
      load_init_log_message(source_id, bigquery_project_id)
    end)

    Logger.info("ETS table started: #{source_id}")
    {:noreply, state}
  end

  def handle_cast(
        {:push, source_id, %LE{injested_at: injested_at, sys_uint: sys_uint} = log_event},
        state
      ) do
    timestamp = injested_at |> Timex.to_datetime() |> DateTime.to_unix(:microsecond)
    :ets.insert(source_id, {{timestamp, sys_uint, 0}, log_event})
    {:noreply, state}
  end

  def handle_info(:prune, state) do
    source_id = state[:source_token]
    {:ok, count} = Counters.log_count(source_id)

    case count > 100 do
      true ->
        for _log <- 101..count do
          log = :ets.first(source_id)
          :ets.delete(source_id, log)
          Counters.decriment(source_id)
        end

        prune()
        {:noreply, state}

      false ->
        prune()
        {:noreply, state}
    end
  end

  def terminate(reason, state) do
    # Do Shutdown Stuff
    Logger.info("Going Down: #{state[:source_token]}")
    reason
  end

  ## Private Functions

  defp load_init_log_message(source_id, bigquery_project_id) do
    log_count = Data.get_log_count(source_id, bigquery_project_id)

    if log_count > 0 do
      unique_integer = System.unique_integer([:monotonic])
      timestamp = System.system_time(:microsecond)
      time_event = {timestamp, unique_integer, 0}
      source_table_string = Atom.to_string(source_id)

      log_message =
        "Initialized and waiting for new events. #{Delimit.number_to_delimited(log_count)} archived and available to explore."

      payload = %{timestamp: timestamp, log_message: log_message}
      log = {time_event, payload}

      push(source_id, log)

      case :ets.info(LogflareWeb.Endpoint) do
        :undefined ->
          Logger.error("Endpoint not up yet! Module: #{__MODULE__}")

        _ ->
          LogflareWeb.Endpoint.broadcast(
            "source:" <> source_table_string,
            "source:#{source_table_string}:new",
            payload
          )
      end
    end
  end

  defp prune() do
    Process.send_after(self(), :prune, @prune_timer)
  end
end
