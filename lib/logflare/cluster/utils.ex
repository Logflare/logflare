defmodule Logflare.Cluster.Utils do
  @min_cluster_size 6

  def node_list_all() do
    [Node.self() | Node.list()]
  end

  def cluster_size() do
    lib_cluster_size = node_list_all() |> Enum.count()

    if lib_cluster_size > @min_cluster_size do
      lib_cluster_size
    else
      @min_cluster_size
    end
  end
end
