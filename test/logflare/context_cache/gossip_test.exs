defmodule Logflare.ContextCache.GossipTest do
  use Logflare.DataCase, async: true
  import ExUnit.CaptureLog
  import Logflare.Factory
  alias Logflare.ContextCache
  alias Logflare.ContextCache.Tombstones
  alias Logflare.Sources

  describe "record_tombstones/1" do
    setup do
      Cachex.clear!(Tombstones.Cache)
      :ok
    end

    test "writes primary keys to the tombstone cache" do
      ContextCache.Gossip.record_tombstones([
        {Sources, 123},
        {Sources, id: 234, other: :info},
        {Sources, %{id: 345, other: :info}},
        {Sources, "uuid-456"}
      ])

      assert Tombstones.Cache.tombstoned?({Sources.Cache, 123})
      assert Tombstones.Cache.tombstoned?({Sources.Cache, 234})
      assert Tombstones.Cache.tombstoned?({Sources.Cache, 345})
      assert Tombstones.Cache.tombstoned?({Sources.Cache, "uuid-456"})
    end

    test "ignores unsupported types gracefully" do
      assert capture_log(fn ->
               ContextCache.Gossip.record_tombstones([{Sources, make_ref()}])
             end) =~ "[warning] Unable to create tombstone for context primary key:"
    end
  end

  describe "maybe_gossip/3" do
    setup do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:logflare, :context_cache_gossip, :multicast, :stop]
        ])

      on_exit(fn -> :telemetry.detach(telemetry_ref) end)
      {:ok, telemetry_ref: telemetry_ref}
    end

    test "emits telemetry on cache miss", %{telemetry_ref: telemetry_ref} do
      insert(:plan, name: "Free")
      source = insert(:source, user: build(:user))

      cache_key = {:get_by, [[token: source.token]]}

      Sources.Cache.get_by(token: source.token)

      assert_receive {[:logflare, :context_cache_gossip, :multicast, :stop], ^telemetry_ref,
                      _measurements, %{cache: Sources.Cache, key: ^cache_key} = metadata}

      assert Map.take(metadata, [:enabled, :max_nodes, :ratio]) == %{
               enabled: true,
               max_nodes: 3,
               ratio: 0.05
             }
    end
  end

  describe "receive_gossip/3" do
    setup do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:logflare, :context_cache_gossip, :receive, :stop]
        ])

      on_exit(fn -> :telemetry.detach(telemetry_ref) end)
      {:ok, telemetry_ref: telemetry_ref}
    end

    test "caches received value", %{telemetry_ref: telemetry_ref} do
      cache_key = {:get, [999]}
      value = %{id: 999, name: "valid"}

      ContextCache.Gossip.receive_gossip(Sources.Cache, cache_key, value)

      assert_receive {[:logflare, :context_cache_gossip, :receive, :stop], ^telemetry_ref,
                      _measurements, %{action: :cached, cache: Sources.Cache, key: ^cache_key}}

      assert Cachex.get!(Sources.Cache, cache_key) == {:cached, value}
    end

    test "refreshes ttl when value is already cached", %{telemetry_ref: telemetry_ref} do
      cache_key = {:get, [111]}
      existing_value = {:cached, %{id: 111, name: "local_data"}}

      Cachex.put(Sources.Cache, cache_key, existing_value)
      ContextCache.Gossip.receive_gossip(Sources.Cache, cache_key, %{id: 111, name: "stale"})

      assert_receive {[:logflare, :context_cache_gossip, :receive, :stop], ^telemetry_ref,
                      _measurements, %{action: :refreshed, cache: Sources.Cache, key: ^cache_key}}

      assert Cachex.get!(Sources.Cache, cache_key) == existing_value
    end

    test "drops received value when record is tombstoned", %{telemetry_ref: telemetry_ref} do
      cache_key = {:get, [222]}
      value = %{id: 222, name: "stale_data"}

      Tombstones.Cache.put_tombstone({Sources.Cache, 222})
      ContextCache.Gossip.receive_gossip(Sources.Cache, cache_key, value)

      assert_receive {[:logflare, :context_cache_gossip, :receive, :stop], ^telemetry_ref,
                      _measurements,
                      %{action: :dropped_stale, cache: Sources.Cache, key: ^cache_key}}

      refute Cachex.get!(Sources.Cache, cache_key)
    end
  end
end
