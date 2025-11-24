defmodule Logflare.Backends.IngestEventQueue.BufferCacheWorker do
  @moduledoc """
  A worker that caches all source-backend buffer statistics periodically for the entire node.

  Caches cluster buffer length of all source-backend queues.
  """
  use GenServer
  alias Logflare.Backends
  require Logger
  @ets_table_mapper :ingest_event_queue_mapping

  @default_interval 2_500

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    state = %{interval: Keyword.get(opts, :interval, @default_interval)}
    Process.send_after(self(), :cache_buffer_lens, state.interval)
    {:ok, state}
  end

  def handle_info(:cache_buffer_lens, state) do
    :ets.foldl(
      fn
        {{sid, bid, _pid}, _tid}, acc when is_map_key(acc, {sid, bid}) ->
          acc

        {{sid, bid, _pid}, _tid}, acc ->
          Backends.cache_local_buffer_lens(sid, bid)
          Map.put(acc, {sid, bid}, true)
      end,
      %{},
      @ets_table_mapper
    )

    Process.send_after(self(), :cache_buffer_lens, state.interval)
    {:noreply, state}
  end
end
