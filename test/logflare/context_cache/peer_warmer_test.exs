defmodule Logflare.ContextCache.PeerWarmerTest do
  use Logflare.DataCase, async: false

  import Cachex.Spec
  import ExUnit.CaptureLog

  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache.Gossip
  alias Logflare.ContextCache.PeerWarmer
  alias Logflare.ContextCache.PeerWarmer.Transfer
  alias Logflare.ContextCache.Tombstones
  alias Logflare.KeyValues
  alias Logflare.KeyValues.Cache.L1
  alias Logflare.Sources

  @stop_event [:logflare, :context_cache, :peer_warm, :stop]

  setup do
    original_config = Application.fetch_env!(:logflare, PeerWarmer)

    Application.put_env(
      :logflare,
      PeerWarmer,
      Keyword.merge(original_config, enabled: true, peer_wait: 10)
    )

    Cachex.clear!(Sources.Cache)
    Cachex.clear!(Tombstones.Cache)
    L1.delete_all!()
    erase_statuses()

    telemetry_ref = :telemetry_test.attach_event_handlers(self(), [@stop_event])

    on_exit(fn ->
      Application.put_env(:logflare, PeerWarmer, original_config)
      :telemetry.detach(telemetry_ref)
      erase_statuses()
    end)

    [telemetry_ref: telemetry_ref]
  end

  describe "copy_from_peer/1 peer selection" do
    test "falls back without contacting peers when disabled" do
      Application.put_env(
        :logflare,
        PeerWarmer,
        Keyword.put(Application.fetch_env!(:logflare, PeerWarmer), :enabled, false)
      )

      reject(ClusterUtils, :peer_list_partial, 2)

      assert :fallback = PeerWarmer.copy_from_peer(Sources.Cache)
    end

    test "falls back when there are no peers", %{telemetry_ref: ref} do
      stub(ClusterUtils, :peer_list_partial, fn _ratio, _max_nodes -> [] end)

      assert :fallback = PeerWarmer.copy_from_peer(Sources.Cache)
      assert_outcome(ref, Sources.Cache, :no_peers)
    end

    test "copies from the ready peer with the most recent warmed_at", %{telemetry_ref: ref} do
      now = DateTime.utc_now()
      newest = DateTime.add(now, -1, :minute)

      stub_peers([
        {:old@host, {:ok, peer_status(warmed_at: DateTime.add(now, -30, :minute))}},
        {:new@host, {:ok, peer_status(warmed_at: newest)}},
        {:booting@host, {:ok, peer_status(state: :warming, warmed_at: nil)}}
      ])

      test_pid = self()

      stub(Transfer, :run, fn node, _store, _target, _import_fun, _timeout ->
        send(test_pid, {:transfer_from, node})
        {:ok, 5}
      end)

      assert {:ok, %{node: :new@host, warmed_at: ^newest, count: 5}} =
               PeerWarmer.copy_from_peer(Sources.Cache)

      assert_received {:transfer_from, :new@host}
      assert_outcome(ref, Sources.Cache, :copied)
    end

    test "falls back when no peer is eligible", %{telemetry_ref: ref} do
      now = DateTime.utc_now()

      stub_peers([
        {:warming@host, {:ok, peer_status(state: :warming, warmed_at: nil)}},
        {:other_version@host, {:ok, peer_status(format_version: 2)}},
        {:empty@host, {:ok, peer_status(size: 0)}},
        {:stale@host, {:ok, peer_status(warmed_at: DateTime.add(now, -2, :hour))}},
        {:not_warmed@host, {:ok, nil}},
        {:old_code@host, {:error, {:exception, :undef, []}}},
        {:down@host, {:error, {:erpc, :noconnection}}}
      ])

      reject(Transfer, :run, 5)

      assert :fallback = PeerWarmer.copy_from_peer(KeyValues.Cache)
      assert_outcome(ref, KeyValues.Cache, :no_eligible_peer)
    end

    test "does not apply a max age to Sources" do
      old = DateTime.add(DateTime.utc_now(), -2, :day)
      stub_peers([{:old@host, {:ok, peer_status(warmed_at: old)}}])
      stub(Transfer, :run, fn _node, _store, _target, _import_fun, _timeout -> {:ok, 1} end)

      assert {:ok, %{warmed_at: ^old}} = PeerWarmer.copy_from_peer(Sources.Cache)
    end

    test "falls back when the transfer fails", %{telemetry_ref: ref} do
      stub_peers([{:peer@host, {:ok, peer_status([])}}])

      stub(Transfer, :run, fn _node, _store, _target, _import_fun, _timeout ->
        {:error, :peer_down}
      end)

      assert :fallback = PeerWarmer.copy_from_peer(Sources.Cache)
      assert_outcome(ref, Sources.Cache, :peer_down)
    end

    test "falls back when the copy crashes", %{telemetry_ref: ref} do
      stub_peers([{:peer@host, {:ok, peer_status([])}}])

      stub(Transfer, :run, fn _node, _store, _target, _import_fun, _timeout ->
        raise "boom"
      end)

      capture_log(fn ->
        assert :fallback = PeerWarmer.copy_from_peer(Sources.Cache)
      end)

      assert_outcome(ref, Sources.Cache, :crashed)
    end
  end

  describe "copy_from_peer/1 importing" do
    test "imports Sources entries except negative, invalidated and already cached ones" do
      [fresh, invalidated, cached] = insert_list(3, :source, user: insert(:user))
      Gossip.record_tombstones([{Sources, invalidated.id}])
      Cachex.put!(Sources.Cache, get_by_key(cached), {:cached, :local_value})

      stub_transfer_of([
        cachex_entry(get_by_key(fresh), {:cached, fresh}),
        cachex_entry({:get_by, [[id: -1]]}, {:cached, nil}),
        cachex_entry(get_by_key(invalidated), {:cached, invalidated}),
        cachex_entry(get_by_key(cached), {:cached, cached})
      ])

      assert {:ok, %{count: 1}} = PeerWarmer.copy_from_peer(Sources.Cache)

      assert {:cached, %{id: fresh_id}} = Cachex.get!(Sources.Cache, get_by_key(fresh))
      assert fresh_id == fresh.id
      assert Cachex.get!(Sources.Cache, get_by_key(invalidated)) == nil
      assert Cachex.get!(Sources.Cache, get_by_key(cached)) == {:cached, :local_value}
    end

    test "keeps the remaining TTL of copied Sources entries" do
      source = insert(:source, user: insert(:user))
      ttl = to_timeout(minute: 10)
      modified = System.system_time(:millisecond) - to_timeout(minute: 4)

      stub_transfer_of([
        entry(
          key: get_by_key(source),
          value: {:cached, source},
          modified: modified,
          expiration: ttl
        )
      ])

      assert {:ok, %{count: 1}} = PeerWarmer.copy_from_peer(Sources.Cache)

      assert {:ok, remaining} = Cachex.ttl(Sources.Cache, get_by_key(source))
      assert remaining <= to_timeout(minute: 6)
      assert remaining > to_timeout(minute: 5)
    end

    test "imports KeyValues entries except negative and invalidated ones" do
      user_id = 1
      other_user_id = 2
      Gossip.record_tombstones([{KeyValues, [user_id: user_id, key: "invalidated"]}])

      stub_transfer_of([
        {{:lookup, [user_id, "fresh", nil]}, %{"v" => 1}},
        {{:lookup, [user_id, "invalidated", nil]}, %{"v" => 2}},
        {{:lookup, [user_id, "missing", nil]}, nil},
        {{:count, user_id}, 3},
        {{:count, other_user_id}, 4}
      ])

      assert {:ok, %{count: 2}} = PeerWarmer.copy_from_peer(KeyValues.Cache)

      assert {:ok, %{"v" => 1}} = L1.fetch({:lookup, [user_id, "fresh", nil]})
      assert {:ok, 4} = L1.fetch({:count, other_user_id})
      refute L1.has_key?({:lookup, [user_id, "invalidated", nil]}) == {:ok, true}
      refute L1.has_key?({:lookup, [user_id, "missing", nil]}) == {:ok, true}
      refute L1.has_key?({:count, user_id}) == {:ok, true}
    end
  end

  describe "status/1" do
    test "is nil until the cache has started warming" do
      assert PeerWarmer.status(Sources.Cache) == nil
    end

    test "is nil for caches that don't support peer warming" do
      assert PeerWarmer.status(Logflare.Users.Cache) == nil
    end

    test "reports the warming state" do
      PeerWarmer.mark_warming(Sources.Cache)

      assert %{state: :warming, warmed_at: nil} = PeerWarmer.status(Sources.Cache)
    end

    test "reports the warmed_at, format version and size once ready" do
      warmed_at = DateTime.utc_now()
      Cachex.put!(Sources.Cache, :some_key, {:cached, %{id: 1}})
      PeerWarmer.mark_ready(Sources.Cache, warmed_at)

      assert %{state: :ready, warmed_at: ^warmed_at, format_version: 1, size: 1} =
               PeerWarmer.status(Sources.Cache)
    end
  end

  defp stub_peers(results) do
    stub(ClusterUtils, :peer_list_partial, fn 1.0, _max_nodes ->
      Enum.map(results, &elem(&1, 0))
    end)

    stub(ClusterUtils, :erpc_multicall, fn _nodes, PeerWarmer, :status, [_cache], _timeout ->
      results
    end)
  end

  defp stub_transfer_of(entries) do
    stub_peers([{:peer@host, {:ok, peer_status([])}}])

    stub(Transfer, :run, fn _node, _store, _target, import_fun, _timeout ->
      {:ok, import_fun.(entries)}
    end)
  end

  defp peer_status(overrides) do
    Map.merge(
      %{state: :ready, warmed_at: DateTime.utc_now(), format_version: 1, size: 10},
      Map.new(overrides)
    )
  end

  defp cachex_entry(key, value) do
    entry(key: key, value: value, modified: System.system_time(:millisecond), expiration: nil)
  end

  defp get_by_key(source), do: {:get_by, [[id: source.id]]}

  defp assert_outcome(ref, cache, outcome) do
    assert_received {@stop_event, ^ref, _measurements, %{cache: ^cache, outcome: ^outcome}}
  end

  defp erase_statuses do
    for cache <- [Sources.Cache, KeyValues.Cache] do
      :persistent_term.erase({PeerWarmer, cache})
    end
  end
end
