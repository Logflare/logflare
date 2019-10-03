defmodule Logflare.Cluster.RedisTest do
  @moduledoc false
  use ExUnit.Case
  alias Logflare.Source.Store

  describe "cluster" do
    test "source store get, increment, reset" do
      key = :redis_test_key
      Store.reset(key)

      nodes =
        LocalCluster.start_nodes(:spawn, 3,
          files: [
            __ENV__.file
          ]
        )

      [node1, node2, node3] = nodes

      assert Node.ping(node1) == :pong
      assert Node.ping(node2) == :pong
      assert Node.ping(node3) == :pong

      Node.spawn(node1, fn ->
        Store.increment(key)
      end)

      Node.spawn(node2, fn ->
        Store.increment(key)
      end)

      Node.spawn(node3, fn ->
        Store.increment(key)
      end)

      Process.sleep(100)
      assert {:ok, "3"} = Store.get(key)
    end
  end
end
