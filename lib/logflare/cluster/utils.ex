defmodule Logflare.Cluster.Utils do
  @moduledoc false
  require Logger

  @spec node_list_all() :: [Node.t()]
  def node_list_all() do
    [Node.self() | Node.list()]
  end

  @spec cluster_size() :: non_neg_integer()
  def cluster_size() do
    lib_cluster_size = actual_cluster_size()
    min_size = env_min_cluster_size()

    if lib_cluster_size >= min_size do
      lib_cluster_size
    else
      Logger.warning("Cluster size is #{lib_cluster_size} but expected #{min_size}",
        cluster_size: lib_cluster_size
      )

      min_size
    end
  end

  @spec actual_cluster_size() :: non_neg_integer()
  def actual_cluster_size() do
    Enum.count(node_list_all())
  end

  defp env_min_cluster_size, do: Application.get_env(:logflare, __MODULE__)[:min_cluster_size]
end
