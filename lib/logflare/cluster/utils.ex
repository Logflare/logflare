defmodule Logflare.Cluster.Utils do
  def node_list_all() do
    [Node.self() | Node.list()]
  end
end
