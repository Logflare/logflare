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
  alias Logflare.Sources.Counters

  require Logger

  @broadcast_every 2_500

  ## Server
  def start_link(args) do
    GenServer.start_link(__MODULE__, args,
      name: Backends.via_source(args[:source], __MODULE__),
      hibernate_after: 5_000,
      spawn_opt: [
        fullsweep_after: 100
      ]
    )
  end

  ## Client
  def init(args) do
    source = Keyword.get(args, :source)

    Process.flag(:trap_exit, true)
    Logger.metadata(source_id: source.token, source_token: source.token)
    touch_every = Enum.random(10..30) * :timer.minutes(1)
    touch(touch_every)
    broadcast()

    Logger.debug("[#{__MODULE__}] Started")

    {:ok,
     %{
       source_token: source.token,
       source_id: source.id,
       touch_every: touch_every
     }}
  end

  def handle_info({:stop_please, reason}, state) do
    {:stop, reason, state}
  end

  def handle_info(:broadcast, state) do
    {:ok, total_cluster_inserts, inserts_since_boot} = broadcast_count(state)

    prev_inserts_since_boot = Counters.get_inserts_since_boot(state.source_token)
    prev_last_cluster_inserts = Counters.get_total_cluster_inserts(state.source_token)
    prev_changed_at = Counters.get_source_changed_at_unix_ms(state.source_token)
    now = System.system_time(:microsecond)

    if prev_inserts_since_boot < inserts_since_boot do
      Counters.increment_inserts_since_boot_count(
        state.source_token,
        inserts_since_boot - prev_inserts_since_boot
      )

      Counters.increment_source_changed_at_unix_ts(
        state.source_token,
        now - prev_changed_at
      )
    end

    if prev_last_cluster_inserts < total_cluster_inserts do
      Counters.increment_total_cluster_inserts_count(
        state.source_token,
        total_cluster_inserts - prev_last_cluster_inserts
      )
    end

    broadcast()

    {:noreply, state}
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

        if NaiveDateTime.diff(now, latest_ts, :millisecond) < :timer.seconds(1) do
          source
          |> Sources.update_source(%{log_events_updated_at: DateTime.utc_now()})
        end
    end

    touch(state.touch_every)
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

  defp broadcast_count(%{source_token: source_token}) do
    current_inserts = Source.Data.get_node_inserts(source_token)

    if current_inserts > Counters.get_inserts_since_boot(source_token) do
      bq_inserts = Source.Data.get_bq_inserts(source_token)
      inserts_payload = %{Node.self() => %{node_inserts: current_inserts, bq_inserts: bq_inserts}}

      PubSubRates.global_broadcast_rate({"inserts", source_token, inserts_payload})
    end

    current_cluster_inserts = PubSubRates.Cache.get_cluster_inserts(source_token)
    last_cluster_inserts = Counters.get_total_cluster_inserts(source_token)

    if current_cluster_inserts > last_cluster_inserts do
      payload = %{log_count: current_cluster_inserts, source_token: source_token}
      Source.ChannelTopics.local_broadcast_log_count(payload)
    end

    {:ok, current_cluster_inserts, current_inserts}
  end

  defp touch(every) do
    Process.send_after(self(), :touch, every)
  end

  defp broadcast() do
    Process.send_after(self(), :broadcast, @broadcast_every)
  end
end
