defmodule Logflare.Backends.IngestEventQueue.BroadcastWorker do
  @moduledoc """
  A worker that broadcasts all source-backend buffer statistics periodically for the entire node.

  Broadcasts cluster buffer length of a given queue (an integer) locally.
  Broadcasts local buffer length of a given queue globally.
  """
  use GenServer
  alias Logflare.PubSubRates
  alias Logflare.Backends
  require Logger
  @ets_table_mapper :ingest_event_queue_mapping

  @default_interval 2_500

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    state = %{interval: Keyword.get(opts, :interval, @default_interval)}
    Process.send_after(self(), :global_broadcast, state.interval)
    Process.send_after(self(), :local_broadcast, state.interval)
    {:ok, state}
  end

  def handle_info(:global_broadcast, state) do
    :ets.foldl(
      fn {{sid, bid, _pid}, _tid}, acc ->
        if is_map_key(acc, {sid, bid}) do
          acc
        else
          Backends.cache_local_buffer_lens(sid, bid)
          Map.put(acc, {sid, bid}, true)
        end
      end,
      %{},
      @ets_table_mapper
    )

    # scale broadcasting interval to cluster size
    cluster_size = Logflare.Cluster.Utils.actual_cluster_size()
    broadcast_interval = max(state.interval, round(:rand.uniform(cluster_size * 100)))

    Process.send_after(self(), :global_broadcast, broadcast_interval)
    {:noreply, state}
  end

  def handle_info(:local_broadcast, state) do
    :ets.foldl(
      fn {{sid, bid, _pid}, _tid}, acc ->
        if is_map_key(acc, {sid, bid}) do
          acc
        else
          Map.put(acc, {sid, bid}, true)
        end
      end,
      %{},
      @ets_table_mapper
    )

    Process.send_after(self(), :local_broadcast, state.interval)
    {:noreply, state}
  end
end
