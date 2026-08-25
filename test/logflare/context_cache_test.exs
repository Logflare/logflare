defmodule Logflare.ContextCacheTest do
  use Logflare.DataCase, async: false

  alias Ecto.Adapters.SQL
  alias Logflare.Backends
  alias Logflare.ContextCache
  alias Logflare.ContextCache.Tombstones
  alias Logflare.ContextCache.TransactionBroadcaster

  defmodule RecordingOps do
    @behaviour Logflare.Cache.Ops
    @behaviour Logflare.ContextCache.Ops

    @impl Logflare.Cache.Ops
    def healthy?(cache), do: record({:healthy?, cache}, true)
    @impl Logflare.Cache.Ops
    def stats(cache), do: record({:stats, cache}, %{})
    @impl Logflare.Cache.Ops
    def reset(cache), do: record({:reset, cache}, :ok)

    @impl Logflare.ContextCache.Ops
    def keys_to_bust(cache, kw), do: record({:keys_to_bust, cache, kw}, [:key])
    @impl Logflare.ContextCache.Ops
    def delete_keys(cache, keys), do: record({:delete_keys, cache, keys}, {:ok, 1})
    @impl Logflare.ContextCache.Ops
    def fetch(cache, key, _getter), do: record({:fetch, cache, key}, :fetched)
    @impl Logflare.ContextCache.Ops
    def update(cache, key, value), do: record({:update, cache, key, value}, :ok)

    defp record(call, result) do
      send(self(), call)
      result
    end
  end

  defmodule CustomImpl.Cache do
    use Logflare.ContextCache, impl: RecordingOps
  end

  test "bust_keys/1 with an empty list" do
    assert {:ok, 0} = ContextCache.bust_keys([])
  end

  describe "context cache with a custom impl module" do
    test "apply_fun/3" do
      assert :fetched = ContextCache.apply_fun(CustomImpl, :get, [1])
      assert_received {:fetch, CustomImpl.Cache, {:get, [1]}}
    end

    test "update/4" do
      assert :ok = ContextCache.update(CustomImpl, :get, [1], :value)
      assert_received {:update, CustomImpl.Cache, {:get, [1]}, :value}
    end

    for {name, bust, expected_kw} <- [
          {"primary key", 5, [id: 5]},
          {"keyword", [source_id: 5], [source_id: 5]}
        ] do
      test "bust_keys/1 with a #{name}" do
        assert {:ok, 1} = ContextCache.bust_keys([{CustomImpl, unquote(bust)}])
        assert_received {:keys_to_bust, CustomImpl.Cache, unquote(expected_kw)}
        assert_received {:delete_keys, CustomImpl.Cache, [:key]}
      end
    end

    test "fetch_consistent/3 retries a value loaded across an invalidation" do
      cache_key = {:get_backend, [System.unique_integer([:positive])]}
      Cachex.del(Backends.Cache, cache_key)
      parent = self()

      task =
        Task.async(fn ->
          ContextCache.fetch_consistent(Backends.Cache, cache_key, fn ->
            send(parent, {:getter_started, self()})

            receive do
              {:return, value} -> value
            end
          end)
        end)

      assert_receive {:getter_started, first_getter}
      ContextCache.Gossip.record_cache_tombstones(Backends.Cache, [cache_key])
      send(first_getter, {:return, :stale})

      assert_receive {:getter_started, second_getter}
      send(second_getter, {:return, :current})

      assert Task.await(task) == :current
      assert {:cached, :current, _generation} = Cachex.get!(Backends.Cache, cache_key)
    end

    test "fetch_consistent/3 retries a list loaded across a generic record invalidation" do
      backend_id = System.unique_integer([:positive])
      cache_key = {:list_backends, [[source_id: System.unique_integer([:positive])]]}
      Cachex.del(Backends.Cache, cache_key)
      parent = self()

      task =
        Task.async(fn ->
          ContextCache.fetch_consistent(Backends.Cache, cache_key, fn ->
            send(parent, {:getter_started, self()})

            receive do
              {:return, value} -> value
            end
          end)
        end)

      assert_receive {:getter_started, first_getter}
      ContextCache.Gossip.record_tombstones([{Backends, backend_id}])
      send(first_getter, {:return, [%{id: backend_id}]})

      assert_receive {:getter_started, second_getter}
      send(second_getter, {:return, []})

      assert Task.await(task) == []
      assert {:cached, [], generation} = Cachex.get!(Backends.Cache, cache_key)

      assert ContextCache.Gossip.cache_value_generation_current?(
               Backends.Cache,
               cache_key,
               generation
             )
    end

    test "fetch_consistent/3 rejects cached records invalidated by primary key" do
      backend_id = System.unique_integer([:positive])
      cache_key = {:list_backends, [[source_id: System.unique_integer([:positive])]]}
      stale_value = [%{id: backend_id}]

      stale_generation =
        ContextCache.Gossip.cache_value_generation(Backends.Cache, cache_key, stale_value)

      Cachex.put(Backends.Cache, cache_key, {:cached, stale_value, stale_generation})
      ContextCache.Gossip.record_tombstones([{Backends, backend_id}])

      assert ContextCache.fetch_consistent(Backends.Cache, cache_key, fn -> [] end) == []
      assert {:cached, [], generation} = Cachex.get!(Backends.Cache, cache_key)

      assert ContextCache.Gossip.cache_value_generation_current?(
               Backends.Cache,
               cache_key,
               generation
             )
    end

    test "fetch_consistent/3 rejects a cached value from an older generation" do
      cache_key = {:get_backend, [System.unique_integer([:positive])]}

      stale_generation =
        ContextCache.Gossip.cache_value_generation(Backends.Cache, cache_key, :stale)

      ContextCache.Gossip.record_cache_tombstones(Backends.Cache, [cache_key])
      Cachex.del(Tombstones.Cache, {Backends.Cache, {:cache_key, cache_key}})

      Cachex.put(Backends.Cache, cache_key, {:cached, :stale, stale_generation})

      assert ContextCache.fetch_consistent(Backends.Cache, cache_key, fn -> :current end) ==
               :current

      assert {:cached, :current, current_generation} =
               Cachex.get!(Backends.Cache, cache_key)

      assert ContextCache.Gossip.cache_value_generation_current?(
               Backends.Cache,
               cache_key,
               current_generation
             )
    end

    test "delete_if_value/3 preserves a newer cache generation" do
      cache_key = {:get_backend, [System.unique_integer([:positive])]}
      stale_value = {:cached, :stale, make_ref()}
      current_value = {:cached, :current, make_ref()}
      Cachex.put(Backends.Cache, cache_key, current_value)

      assert :ok = ContextCache.delete_if_value(Backends.Cache, cache_key, stale_value)
      assert Cachex.get!(Backends.Cache, cache_key) == current_value

      assert :ok = ContextCache.delete_if_value(Backends.Cache, cache_key, current_value)
      refute Cachex.get!(Backends.Cache, cache_key)
    end
  end

  describe "unboxed transaction" do
    setup do
      on_exit(fn ->
        SQL.Sandbox.unboxed_run(Logflare.Repo, fn ->
          for u <- Logflare.Repo.all(Logflare.User) do
            Logflare.Repo.delete(u)
          end
        end)
      end)

      :ok
    end

    test "TransactionBroadcaster subscribes to wal and broadcasts transactions" do
      ContextCache.CacheBuster.subscribe_to_transactions()
      start_supervised!({TransactionBroadcaster, interval: 100})
      :timer.sleep(200)

      SQL.Sandbox.unboxed_run(Logflare.Repo, fn ->
        insert(:user)
      end)

      :timer.sleep(500)
      assert_received %Cainophile.Changes.Transaction{}
    end
  end
end
