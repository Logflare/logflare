defmodule Logflare.Backends.IngestEventQueue.BroadcastWorker do
  @moduledoc """
  A worker that broadcasts all source-backend buffer statistics periodically for the entire node.

  Broadcasts cluster buffer length of a given queue (an integer) locally.
  Broadcasts local buffer length of a given queue globally.
  """
  use GenServer
  alias Logflare.Source
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
          global_broadcast_producer_buffer_len({sid, bid})
          Map.put(acc, {sid, bid}, true)
        end
      end,
      %{},
      @ets_table_mapper
    )

    Process.send_after(self(), :global_broadcast, state.interval * 2)
    {:noreply, state}
  end

  def handle_info(:local_broadcast, state) do
    :ets.foldl(
      fn {{sid, bid, _pid}, _tid}, acc ->
        if is_map_key(acc, {sid, bid}) do
          acc
        else
          local_broadcast_cluster_length({sid, bid})
          Map.put(acc, {sid, bid}, true)
        end
      end,
      %{},
      @ets_table_mapper
    )

    Process.send_after(self(), :local_broadcast, state.interval)
    {:noreply, state}
  end

  defp global_broadcast_producer_buffer_len({source_id, backend_id}) do
    len = Backends.get_and_cache_local_pending_buffer_len(source_id, backend_id)

    local_buffer = %{Node.self() => %{len: len}}
    PubSubRates.global_broadcast_rate({:buffers, source_id, backend_id, local_buffer})
  end

  defp local_broadcast_cluster_length({source_id, backend_id}) do
    payload = %{
      buffer: PubSubRates.Cache.get_cluster_buffers(source_id),
      source_id: source_id,
      backend_id: backend_id
    }

    Source.ChannelTopics.local_broadcast_buffer(payload)
  end
end
