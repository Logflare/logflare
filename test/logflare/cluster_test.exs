defmodule Logflare.ClusterTest do
  use ExUnit.Case, async: true
  alias Logflare.Cluster

  test "Cluster.Utils.node_list_all/0" do
    assert [_ | _] = Cluster.Utils.node_list_all()
  end

  test "cluster_size/0, actual_cluster_size/0" do
    assert Cluster.Utils.cluster_size() == Cluster.Utils.actual_cluster_size()
  end

  describe "Cluster.Utils.erpc_call/5" do
    test "returns the result of the call" do
      assert {:ok, 3} = Cluster.Utils.erpc_call(node(), Kernel, :+, [1, 2])
    end

    test "returns an exception raised by the call instead of raising it" do
      assert {:error, {:exception, :boom, _stacktrace}} =
               Cluster.Utils.erpc_call(node(), :erlang, :error, [:boom])
    end

    test "returns a call to an undefined function instead of raising it" do
      assert {:error, {:exception, :undef, _stacktrace}} =
               Cluster.Utils.erpc_call(node(), Cluster.Utils, :undefined_function, [])
    end

    test "returns a failure to reach the node instead of raising it" do
      assert {:error, {:erpc, :noconnection}} =
               Cluster.Utils.erpc_call(:"missing@127.0.0.1", Kernel, :+, [1, 2])
    end
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
