defmodule Logflare.ClusterTest do
  use ExUnit.Case, async: true
  alias Logflare.Cluster

  test "Cluster.Utils.node_list_all/0" do
    assert [_ | _] = Cluster.Utils.node_list_all()
  end

  test "cluster_size/0, actual_cluster_size/0" do
    assert Cluster.Utils.cluster_size() == Cluster.Utils.actual_cluster_size()
  end

  describe "Cluster.Utils" do
    setup do
      config = Application.get_env(:logflare, Cluster.Utils)
      Application.put_env(:logflare, Cluster.Utils, min_cluster_size: 12)

      on_exit(fn ->
        Application.put_env(:logflare, Cluster.Utils, config)
      end)
    end

    test "cluster_size/0, actual_cluster_size/0" do
      assert Cluster.Utils.cluster_size() == 12
      assert Cluster.Utils.actual_cluster_size() == 1
    end
  end
end
