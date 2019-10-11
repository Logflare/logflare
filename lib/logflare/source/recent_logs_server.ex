defmodule Logflare.Source.RecentLogsServer do
  @moduledoc """
  Manages the individual table for the source. Limits things in the table to 1000. Manages TTL for
  things in the table. Handles loading the table from the disk if found on startup.
  """
  use Publicist
  use TypedStruct

  typedstruct do
    field :source_id, atom(), enforce: true
    field :notifications_every, integer(), default: 60_000
    field :inserts_since_boot, integer(), default: 0
    field :bigquery_project_id, atom()
    field :bigquery_dataset_id, binary()
    field :bq_total_on_boot, integer(), default: 0
    field :total_cluster_inserts, integer(), default: 0
  end

  use GenServer

  alias Logflare.Sources.Counters
  alias Logflare.Google.{BigQuery, BigQuery.GenUtils}
  alias Number.Delimit
  alias Logflare.Source.BigQuery.{Schema, Pipeline, Buffer}
  alias Logflare.Source.{Data, EmailNotificationServer, TextNotificationServer, RateCounterServer}
  alias Logflare.LogEvent, as: LE
  alias Logflare.Source
  alias Logflare.Logs.SearchQueryExecutor
  alias __MODULE__, as: RLS

  require Logger

  @prune_timer 1_000
  @broadcast_every 250

  def start_link(%__MODULE__{source_id: source_id} = rls) when is_atom(source_id) do
    GenServer.start_link(__MODULE__, rls, name: source_id)
  end

  ## Client
  @spec init(RLS.t()) :: {:ok, RLS.t(), {:continue, :boot}}
  def init(rls) do
    Process.flag(:trap_exit, true)
    prune()

    broadcast()

    {:ok, rls, {:continue, :boot}}
  end

  def push(source_id, %LE{} = log_event) do
    GenServer.cast(source_id, {:push, source_id, log_event})
  end

  ## Server

  def handle_continue(:boot, %__MODULE__{source_id: source_id} = rls) when is_atom(source_id) do
    %{
      user_id: user_id,
      bigquery_table_ttl: bigquery_table_ttl,
      bigquery_project_id: bigquery_project_id,
      bigquery_dataset_location: bigquery_dataset_location,
      bigquery_dataset_id: bigquery_dataset_id
    } = GenUtils.get_bq_user_info(source_id)

    BigQuery.init_table!(
      user_id,
      source_id,
      bigquery_project_id,
      bigquery_table_ttl,
      bigquery_dataset_location,
      bigquery_dataset_id
    )

    table_args = [:named_table, :ordered_set, :public]
    :ets.new(source_id, table_args)

    Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
      load_init_log_message(source_id, bigquery_project_id)
    end)

    # {:ok, bq_count} = load_init_log_message(source_id, bigquery_project_id)

    rls = %{
      rls
      | bigquery_project_id: bigquery_project_id,
        bigquery_dataset_id: bigquery_dataset_id,
        bq_total_on_boot: 0
    }

    children = [
      {RateCounterServer, rls},
      {EmailNotificationServer, rls},
      {TextNotificationServer, rls},
      {Buffer, rls},
      {Schema, rls},
      {Pipeline, rls},
      {SearchQueryExecutor, rls}
    ]

    Supervisor.start_link(children, strategy: :one_for_all)

    init_metadata = %{source_token: "#{source_id}", log_count: 0, bq_count: bq_count, inserts: 0}

    Phoenix.Tracker.track(Logflare.Tracker, self(), source_id, Node.self(), init_metadata)

    Logger.info("RecentLogsServer started: #{source_id}")
    {:noreply, rls}
  end

  def handle_cast({:push, source_id, %LE{ingested_at: inj_at, sys_uint: sys_uint} = le}, state) do
    timestamp = inj_at |> Timex.to_datetime() |> DateTime.to_unix(:microsecond)
    :ets.insert(source_id, {{timestamp, sys_uint, 0}, le})
    {:noreply, state}
  end

  def handle_info({:stop_please, reason}, state) do
    {:stop, reason, state}
  end

  def handle_info(:broadcast, state) do
    update_tracker(state)
    {:ok, total_cluster_inserts} = broadcast_count(state)
    broadcast()
    {:noreply, %{state | total_cluster_inserts: total_cluster_inserts}}
  end

  def handle_info(:prune, %__MODULE__{source_id: source_id} = state) do
    count = Data.get_ets_count(source_id)

    case count > 100 do
      true ->
        for _log <- 101..count do
          log = :ets.first(source_id)

          if :ets.delete(source_id, log) == true do
            Counters.decriment(source_id)
          end
        end

        prune()
        {:noreply, state}

      false ->
        prune()
        {:noreply, state}
    end
  end

  def terminate(reason, %__MODULE__{} = state) do
    # Do Shutdown Stuff
    Logger.info("Going Down: #{state.source_id}")
    reason
  end

  ## Private Functions
  defp broadcast_count(state) do
    p =
      Phoenix.Tracker.list(Logflare.Tracker, state.source_id)
      |> merge_metadata

    {_, payload} =
      Map.get_and_update(p, :log_count, fn current_value ->
        {current_value, p.bq_count + p.inserts}
      end)

    if payload.log_count > state.total_cluster_inserts do
      Source.ChannelTopics.broadcast_log_count(payload)
    end

    {:ok, payload.log_count}
  end

  defp update_tracker(state) do
    pid = Process.whereis(state.source_id)

    bq_count = state.bq_total_on_boot
    inserts = Data.get_inserts(state.source_id)

    payload = %{
      source_token: state.source_id,
      bq_count: bq_count,
      inserts: inserts,
      log_count: 0
    }

    Phoenix.Tracker.update(Logflare.Tracker, pid, state.source_id, Node.self(), payload)
  end

  def merge_metadata(list) do
    payload = {:noop, %{inserts: 0, bq_count: 0}}

    {:noop, data} =
      Enum.reduce(list, payload, fn {_, y}, {_, acc} ->
        total = y.inserts + acc.inserts
        bq_count = if y.bq_count > acc.bq_count, do: y.bq_count, else: acc.bq_count

        {:noop, %{y | inserts: total, bq_count: bq_count}}
      end)

    data
  end

  defp load_init_log_message(source_id, bigquery_project_id) do
    log_count = Data.get_log_count(source_id, bigquery_project_id)

    if log_count > 0 do
      message =
        "Initialized on node #{Node.self()}. Waiting for new events. #{
          Delimit.number_to_delimited(log_count)
        } available to explore & search."

      log_event = LE.make(%{"message" => message}, %{source: %Source{token: source_id}})
      push(source_id, log_event)

      Source.ChannelTopics.broadcast_new(log_event)
    end

    {:ok, log_count}
  end

  defp prune() do
    Process.send_after(self(), :prune, @prune_timer)
  end

  defp broadcast() do
    Process.send_after(self(), :broadcast, @broadcast_every)
  end
end
