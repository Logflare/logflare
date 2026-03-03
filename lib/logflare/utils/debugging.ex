defmodule Logflare.Utils.Debugging do
  @moduledoc false
  alias Logflare.Backends.IngestEventQueue

  def list_counts(source_id) do
    :erpc.multicall(all_nodes(), __MODULE__, :list_counts_callback, [source_id], 5000)
  end

  def list_counts_callback(source_id) do
    {Node.self(), IngestEventQueue.list_counts({source_id, nil})}
  end

  def list_pending_counts(source_id) do
    :erpc.multicall(all_nodes(), __MODULE__, :list_pending_counts_callback, [source_id], 5000)
  end

  defp all_nodes, do: [Node.self() | Node.list()]

  def list_pending_counts_callback(source_id) do
    {Node.self(), IngestEventQueue.list_pending_counts({source_id, nil})}
  end
end
