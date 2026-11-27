defmodule Logflare.Utils.Debugging do
  @moduledoc false

  alias Logflare.Backends
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Sources

  def list_counts(source_id) do
    :erpc.multicall(all_nodes(), __MODULE__, :list_counts_callback, [source_id], 5000)
  end

  def list_counts_callback(source_id) do
    source = Sources.Cache.get_by_id(source_id)
    queue_key = Backends.system_default_ingest_queue_key(source)
    {Node.self(), IngestEventQueue.list_counts(queue_key)}
  end

  def list_pending_counts(source_id) do
    :erpc.multicall(all_nodes(), __MODULE__, :list_pending_counts_callback, [source_id], 5000)
  end

  def list_pending_counts_callback(source_id) do
    source = Sources.Cache.get_by_id(source_id)
    queue_key = Backends.system_default_ingest_queue_key(source)
    {Node.self(), IngestEventQueue.list_pending_counts(queue_key)}
  end

  defp all_nodes, do: [Node.self() | Node.list()]
end
