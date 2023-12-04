defmodule Logflare.ContextCacheTest do
  use Logflare.DataCase, async: false

  import Logflare.Factory

  alias Logflare.ContextCache
  alias Logflare.Sources
  alias Logflare.Cluster.CacheWarmer

  describe "warming" do
    setup do
      :ok
    end
    test "retrieve cache data from different node" do
      user = insert(:user)
      [node1] = LocalCluster.start_nodes(:initial, 1,[ files: [ __ENV__.file ] ])

      # fetch from cache
      Node.spawn(node1, fn ->
        Logflare.Users.Cache.get(user.id)
      end)
      :timer.sleep(250)

      reject(&Logflare.Repo.get/2)
      [node2] = LocalCluster.start_nodes(:new, 1,[ files: [ __ENV__.file ] ])
      # manually trigger cache warming
      pid = self()
      :timer.sleep(500)
      Node.spawn(node2, fn ->
        # dbg(Cachex)
        {:ok, count} = Cachex.count(Logflare.Users.Cache)
        send(pid, {:count, count})
      end)
      :timer.sleep(500)
      assert_receive {:count, 1}
    end

  end

  describe "functionality" do
    setup do
      user = insert(:user)
      insert(:plan, name: "Free")
      source = insert(:source, user: user)
      args = [token: source.token]
      source = Sources.Cache.get_by(args)
      fun = :get_by
      cache_key = {fun, [args]}
      %{source: source, cache_key: cache_key}
    end

    test "cache_name/1" do
      assert Sources.Cache == ContextCache.cache_name(Sources)
    end

    test "apply_fun/3", %{cache_key: cache_key} do
      # apply_fun was called in the setup when we called `Sources.Cache.get_by/1`
      # here's we're making sure it did get cached correctly
      assert {:cached, %Logflare.Source{}} = Cachex.get!(Sources.Cache, cache_key)
    end

    test "bust_keys/1", %{source: source, cache_key: cache_key} do
      assert {:ok, :busted} = ContextCache.bust_keys([{Sources, source.id}])
      assert is_nil(Cachex.get!(Sources.Cache, cache_key))
      match = {:entry, {{Sources, source.id}, :_}, :_, :_, :"$1"}
      assert [] = :ets.match(ContextCache, match)
    end

  end
end
