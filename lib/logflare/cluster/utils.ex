defmodule Logflare.Cluster.Utils do
  @moduledoc false
  require Logger

  defp env_min_cluster_size, do: Application.get_env(:logflare, __MODULE__)[:min_cluster_size]

  def node_list_all() do
    [Node.self() | Node.list()]
  end

  def cluster_size() do
    lib_cluster_size = node_list_all() |> Enum.count()

    if lib_cluster_size >= env_min_cluster_size() do
      lib_cluster_size
    else
      Logger.error("Cluster size is #{lib_cluster_size} but expected #{env_min_cluster_size()}",
        cluster_size: lib_cluster_size
      )

      @min_cluster_size
    end
  end

  def actual_cluster_size() do
    Enum.count(node_list_all())
  end
end
