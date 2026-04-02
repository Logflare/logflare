defmodule Logflare.ContextCacheTest do
  use Logflare.DataCase, async: false

  alias Ecto.Adapters.SQL
  alias Logflare.ContextCache
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
      assert {:cached, [%Backend{}]} = Cachex.get!(Backends.Cache, cache_key)

      assert {:ok, 1} = ContextCache.bust_keys([{Backends, backend.id}])
      assert is_nil(Cachex.get!(Backends.Cache, cache_key))
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

  describe "gossip" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user)
      source = insert(:source, user: user)
      %{source: source, user: user}
    end

    setup do
      Cachex.clear!(:wal_tombstones)
      Cachex.clear!(Sources.Cache)

      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:logflare, :context_cache_gossip, :multicast, :stop],
          [:logflare, :context_cache_gossip, :receive_multicast, :stop]
        ])

      on_exit(fn -> :telemetry.detach(telemetry_ref) end)
      {:ok, telemetry_ref: telemetry_ref}
    end

    test "record_tombstones/1 writes primary keys and :not_found to the tombstone cache" do
      ContextCache.record_tombstones([
        {Sources, 123},
        {Sources, "uuid-456"},
        {Sources, id: 234, other: :info}
      ])

      assert {:ok, true} == Cachex.exists?(:wal_tombstones, {Sources.Cache, 123})
      assert {:ok, true} == Cachex.exists?(:wal_tombstones, {Sources.Cache, "uuid-456"})
      assert {:ok, true} == Cachex.exists?(:wal_tombstones, {Sources.Cache, 234})
    end

    test "record_tombstones/1 ignores unsupported types gracefully" do
      ContextCache.record_tombstones([{Sources, %{id: 123}}])
      assert Cachex.size(:wal_tombstones) == {:ok, _size = 0}
    end

    test "maybe_broadcast/3 emits telemetry on cache miss", %{
      source: source,
      telemetry_ref: telemetry_ref
    } do
      Sources.Cache.get_by(token: source.token)

      assert_receive {[:logflare, :context_cache_gossip, :multicast, :stop], ^telemetry_ref,
                      _measurements, %{cache: Sources.Cache} = metadata}

      assert metadata.enabled == true
      assert metadata.max_nodes == 3
      assert metadata.ratio == 0.05
    end

    test "receive_broadcast/3 inserts valid broadcast when cache is empty and emits telemetry", %{
      telemetry_ref: telemetry_ref
    } do
      cache_key = {:get, [999]}
      value = %{id: 999, name: "valid"}

      ContextCache.receive_multicast(Sources.Cache, cache_key, value)

      assert Cachex.get!(Sources.Cache, cache_key) == {:cached, value}

      assert_receive {[:logflare, :context_cache_gossip, :receive_multicast, :stop],
                      ^telemetry_ref, _measurements, metadata}

      assert metadata.action == :cached
      assert metadata.cache == Sources.Cache
    end

    test "receive_broadcast/3 drops broadcast if the local node already has the key cached", %{
      telemetry_ref: telemetry_ref
    } do
      cache_key = {:get, [111]}
      existing_value = {:cached, %{id: 111, name: "local_data"}}
      Cachex.put(Sources.Cache, cache_key, existing_value)
      ContextCache.receive_multicast(Sources.Cache, cache_key, %{id: 111, name: "stale"})

      assert Cachex.get!(Sources.Cache, cache_key) == existing_value

      assert_receive {[:logflare, :context_cache_gossip, :receive_multicast, :stop],
                      ^telemetry_ref, _measurements, metadata}

      assert metadata.action == :dropped_exists
      assert metadata.cache == Sources.Cache
    end
  end
end
