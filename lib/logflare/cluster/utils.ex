defmodule Logflare.Cluster.Utils do
  @moduledoc false

  @spec node_list_all() :: [Node.t()]
  def node_list_all do
    [Node.self() | Node.list()]
  end

  @doc """
  Returns a random subset of the current cluster peers
  based on the given ratio and maximum number of nodes.
  """
  @spec peer_list_partial(float, pos_integer) :: [Node.t()]
  def peer_list_partial(ratio, max_nodes) do
    peers = Node.list()
    peer_count = length(peers)

    if peer_count == 0 do
      []
    else
      target_count = min(ceil(peer_count * ratio), max_nodes)
      Enum.take_random(peers, target_count)
    end
  end

  @spec cluster_size() :: non_neg_integer()
  def cluster_size do
    max(actual_cluster_size(), min_cluster_size())
  end

  @spec actual_cluster_size() :: non_neg_integer()
  def actual_cluster_size, do: Enum.count(node_list_all())

  def min_cluster_size, do: Application.get_env(:logflare, __MODULE__)[:min_cluster_size]

  @doc """
  Convenience function for `:rpc.multicall/3` with default timeout set instead of `:infinity`.
  """
  @spec rpc_multicall(module(), atom(), [term()], non_neg_integer()) :: term()
  def rpc_multicall(mod, func, args, timeout \\ 5_000) do
    :rpc.multicall(node_list_all(), mod, func, args, timeout)
  end

  @spec rpc_multicast(module(), atom(), [term()]) :: term()
  def rpc_multicast(mod, func, args) do
    :erpc.multicast(node_list_all(), mod, func, args)
  end

  @spec rpc_call(node(), function(), non_neg_integer()) :: term()
  def rpc_call(node, func, timeout \\ 5_000) do
    :erpc.call(node, func, timeout)
  end
end
