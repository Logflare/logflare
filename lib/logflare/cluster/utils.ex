defmodule Logflare.Cluster.Utils do
  @moduledoc false
  require Logger

  @spec node_list_all() :: [Node.t()]
  def node_list_all() do
    [Node.self() | Node.list()]
  end

  @spec cluster_size() :: non_neg_integer()
  def cluster_size() do
    max(actual_cluster_size(), min_cluster_size())
  end

  @spec actual_cluster_size() :: non_neg_integer()
  def actual_cluster_size(), do: Enum.count(node_list_all())

  def min_cluster_size, do: Application.get_env(:logflare, __MODULE__)[:min_cluster_size]
end
