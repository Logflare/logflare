defmodule Logflare.Cluster.Utils do
  @moduledoc false

  require Logger

  @spec node_list_all() :: [Node.t()]
  def node_list_all do
    [Node.self() | Node.list()]
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
