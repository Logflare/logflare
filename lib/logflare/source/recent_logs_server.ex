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
  @broadcast_every 1_500

  ## Server
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: Backends.via_source(args[:source], __MODULE__))
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
       source_id: source.id
     }}
  end

  def handle_info({:stop_please, reason}, state) do
    {:stop, reason, state}
  end

  def handle_info(:broadcast, state) do
    {:ok, total_cluster_inserts, inserts_since_boot} = broadcast_count(state)

    broadcast()

    {:noreply,
     %{
       state
       | total_cluster_inserts: total_cluster_inserts,
         inserts_since_boot: inserts_since_boot
     }}
  end

  def handle_info(:touch, %{source_id: source_id, source_token: source_token} = state) do
    source_id
    |> Sources.Cache.get_by_id()
    |> Backends.list_recent_logs()
    |> Enum.reverse()
    |> case do
      [] ->
        :noop

      [log_event | _] ->
        now = NaiveDateTime.utc_now()

        if NaiveDateTime.diff(now, log_event.ingested_at, :millisecond) < @touch_timer do
          Sources.Cache.get_by(token: source_token)
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

      Phoenix.PubSub.broadcast(
        Logflare.PubSub,
        "inserts",
        {:inserts, source_token, inserts_payload}
      )
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
