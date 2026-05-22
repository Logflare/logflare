defmodule Logflare.Backends.IngestEventQueue.BufferCacheWorker do
  @moduledoc """
  A worker that caches all source-backend buffer statistics periodically for the entire node.

  Caches cluster buffer length of all source-backend queues.
  """
  use GenServer

  alias Logflare.Backends
  alias Logflare.Backends.IngestEventQueue

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
    seen =
      :ets.foldl(
        fn
          {{sid, bid}, _pid, _tid}, acc when is_map_key(acc, {sid, bid}) ->
            acc

          {{sid, bid}, _pid, _tid}, acc ->
            Backends.cache_local_buffer_lens(sid, bid)
            Map.put(acc, {sid, bid}, true)
        end,
        %{},
        @ets_table_mapper
      )

    emit_depth_telemetry(seen)

    Process.send_after(self(), :cache_buffer_lens, state.interval)
    {:noreply, state}
  end

  @spec emit_depth_telemetry(map()) :: :ok
  defp emit_depth_telemetry(seen) do
    by_type =
      Enum.reduce(seen, %{}, fn {{sid, bid}, _}, acc ->
        case IngestEventQueue.total_pending({sid, bid}) do
          n when is_integer(n) and n >= 0 ->
            type = lookup_backend_type(bid)
            Map.update(acc, type, n, &(&1 + n))

          _ ->
            acc
        end
      end)

    for {backend_type, count} <- by_type do
      :telemetry.execute(
        [:logflare, :backends, :ingest_event_queue, :depth],
        %{count: count},
        %{backend_type: backend_type}
      )
    end

    :ok
  end

  defp lookup_backend_type(nil), do: :unknown

  defp lookup_backend_type(bid) do
    case Backends.Cache.get_backend(bid) do
      %{type: type} when is_atom(type) -> type
      _ -> :unknown
    end
  end
end
