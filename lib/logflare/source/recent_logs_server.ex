defmodule Logflare.Source.RecentLogsServer do
  @moduledoc """
  Manages the individual table for the source. Limits things in the table to 1000. Manages TTL for
  things in the table. Handles loading the table from the disk if found on startup.
  """
  use TypedStruct
  use GenServer

  alias Logflare.Sources
  alias Logflare.PubSubRates
  alias Logflare.Backends
  alias Logflare.Sources
  alias Logflare.Source

  require Logger

  @touch_timer :timer.minutes(45)
  @broadcast_every 1_800

  ## Server
  def start_link(args) do
    GenServer.start_link(__MODULE__, args,
      name: Backends.via_source(args[:source], __MODULE__),
      spawn_opt: [
        fullsweep_after: 100
      ],
      hibernate_after: 5_000
    )
  end

  ## Client
  def init(args) do
    source = Keyword.get(args, :source)

    Process.flag(:trap_exit, true)
    Logger.metadata(source_id: source.token, source_token: source.token)

    touch()
    broadcast()

    Logger.debug("[#{__MODULE__}] Started")

    {:ok,
     %{
       inserts_since_boot: 0,
       total_cluster_inserts: 0,
       source_token: source.token,
       source_id: source.id,
       changed_at: 0
     }}
  end

  def get_changed_at(source) do
    Backends.via_source(source, __MODULE__)
    |> GenServer.whereis()
    |> case do
      nil ->
        0

      pid ->
        GenServer.call(pid, :get_changed_at)
    end
  end

  def handle_call(:get_changed_at, _caller, state), do: {:reply, state.changed_at, state}

  def handle_info({:stop_please, reason}, state) do
    {:stop, reason, state}
  end

  def handle_info(:broadcast, state) do
    {:ok, total_cluster_inserts, inserts_since_boot} = broadcast_count(state)

    changed_at =
      if state.inserts_since_boot < inserts_since_boot do
        System.system_time(:microsecond)
      else
        state.changed_at
      end

    broadcast()

    {:noreply,
     %{
       state
       | total_cluster_inserts: total_cluster_inserts,
         inserts_since_boot: inserts_since_boot,
         changed_at: changed_at
     }}
  end

  def handle_info(:touch, %{source_id: source_id} = state) do
    source =
      source_id
      |> Sources.Cache.get_by_id()

    Backends.list_recent_logs_local(source)
    |> Enum.reverse()
    |> case do
      [] ->
        :noop

      [_ | _] = events ->
        now = NaiveDateTime.utc_now()
        latest_ts = Enum.map(events, & &1.ingested_at) |> Enum.max(NaiveDateTime)

        if NaiveDateTime.diff(now, latest_ts, :millisecond) < @touch_timer do
          source
          |> Sources.update_source(%{log_events_updated_at: DateTime.utc_now()})
        end
    end

    touch()
    {:noreply, state}
  end

  def handle_info({:EXIT, _from, _reason}, state) do
    {:noreply, state}
  end

  def handle_info(message, state) do
    Logger.warning("[#{__MODULE__}] Unhandled message: #{inspect(message)}")

    {:noreply, state}
  end

  def terminate(reason, _state) do
    Logger.info("[#{__MODULE__}] Going Down: #{inspect(reason)}")
    reason
  end

  ## Private Functions

  defp broadcast_count(
         %{source_token: source_token, inserts_since_boot: inserts_since_boot} = state
       ) do
    current_inserts = Source.Data.get_node_inserts(source_token)

    if current_inserts > inserts_since_boot do
      bq_inserts = Source.Data.get_bq_inserts(source_token)
      inserts_payload = %{Node.self() => %{node_inserts: current_inserts, bq_inserts: bq_inserts}}

      PubSubRates.global_broadcast_rate({"inserts", source_token, inserts_payload})
    end

    current_cluster_inserts = PubSubRates.Cache.get_cluster_inserts(state.source_token)
    last_cluster_inserts = state.total_cluster_inserts

    if current_cluster_inserts > last_cluster_inserts do
      payload = %{log_count: current_cluster_inserts, source_token: state.source_token}
      Source.ChannelTopics.local_broadcast_log_count(payload)
    end

    {:ok, current_cluster_inserts, current_inserts}
  end

  defp touch() do
    rand = Enum.random(0..30) * :timer.minutes(1)

    every = rand + @touch_timer

    Process.send_after(self(), :touch, every)
  end

  defp broadcast() do
    Process.send_after(self(), :broadcast, @broadcast_every)
  end
end
