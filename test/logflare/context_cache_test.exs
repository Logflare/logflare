defmodule Logflare.ContextCacheTest do
  use Logflare.DataCase, async: false

  alias Ecto.Adapters.SQL
  alias Logflare.ContextCache
  alias Logflare.ContextCache.Tombstones
  alias Logflare.ContextCache.TransactionBroadcaster
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Auth

  describe "ContextCache" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user)
      source = insert(:source, user: user)
      %{source: source, user: user}
    end

    test "bust_keys/1, does nothing for empty list" do
      assert {:ok, 0} = ContextCache.bust_keys([])
    end

    test "apply_fun/3,  bust_keys/1 by :id field of value", %{source: source} do
      Sources.Cache.get_by(token: source.token)
      cache_key = {:get_by, [[token: source.token]]}
      assert {:cached, %Source{}} = Cachex.get!(Sources.Cache, cache_key)

      assert {:ok, 1} = ContextCache.bust_keys([{Sources, source.id}])
      assert is_nil(Cachex.get!(Sources.Cache, cache_key))
    end

    test "apply_fun/3,  bust_keys/1 by :id field of value for :ok tuple", %{user: user} do
      {:ok, key} = Auth.create_access_token(user)
      assert {:ok, _token, _user} = Auth.Cache.verify_access_token(key.token)
      cache_key = {:verify_access_token, [key.token]}
      assert {:cached, {:ok, %_{}, _user}} = Cachex.get!(Auth.Cache, cache_key)

      assert {:ok, 1} = ContextCache.bust_keys([{Auth, key.id}])
      assert is_nil(Cachex.get!(Auth.Cache, cache_key))
    end

    test "apply_fun/3, bust_keys/1 if primary key is in list of returned structs", %{
      source: source
    } do
      backend = insert(:backend, sources: [source])
      Backends.Cache.list_backends(source_id: source.id)
      cache_key = {:list_backends, [[source_id: source.id]]}
      assert {:cached, [%Backend{}], _generation} = Cachex.get!(Backends.Cache, cache_key)

      assert {:ok, 1} = ContextCache.bust_keys([{Backends, backend.id}])
      assert is_nil(Cachex.get!(Backends.Cache, cache_key))
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

    test "fetch_consistent/3 rejects a cached value from an older generation" do
      cache_key = {:get_backend, [System.unique_integer([:positive])]}

      stale_generation =
        ContextCache.Gossip.cache_invalidation_generation(Backends.Cache, cache_key)

      ContextCache.Gossip.record_cache_tombstones(Backends.Cache, [cache_key])
      Cachex.del(Tombstones.Cache, {Backends.Cache, {:cache_key, cache_key}})

      Cachex.put(Backends.Cache, cache_key, {:cached, :stale, stale_generation})

      assert ContextCache.fetch_consistent(Backends.Cache, cache_key, fn -> :current end) ==
               :current

      assert {:cached, :current, current_generation} =
               Cachex.get!(Backends.Cache, cache_key)

      assert current_generation ==
               ContextCache.Gossip.cache_invalidation_generation(Backends.Cache, cache_key)
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
