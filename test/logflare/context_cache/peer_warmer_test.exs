defmodule Logflare.ContextCache.PeerWarmerTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache.Gossip
  alias Logflare.ContextCache.PeerWarmer
  alias Logflare.ContextCache.PeerWarmer.Transfer
  alias Logflare.ContextCache.Tombstones
  alias Logflare.KeyValues
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
    Cachex.clear!(KeyValues.Cache)
    Cachex.clear!(Tombstones.Cache)
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

    test "falls back without contacting peers when the CacheBuster is not running", %{
      telemetry_ref: ref
    } do
      Application.put_env(
        :logflare,
        PeerWarmer,
        Keyword.put(Application.fetch_env!(:logflare, PeerWarmer), :await_cache_buster, true)
      )

      refute Process.whereis(Logflare.ContextCache.CacheBuster)
      reject(ClusterUtils, :peer_list_partial, 2)

      assert :fallback = PeerWarmer.copy_from_peer(Sources.Cache)
      assert_outcome(ref, Sources.Cache, :cache_buster_not_started)
    end

    test "asks peers one after another and copies from the first that accepts", %{
      telemetry_ref: ref
    } do
      warmed_at = DateTime.add(DateTime.utc_now(), -10, :minute)
      expected_guard = {PeerWarmer, :source_status, [Sources.Cache, release_version()]}

      stub_attempts(
        refusing@host: {:error, :not_eligible},
        accepting@host: {:ok, 5, %{warmed_at: warmed_at}},
        unused@host: {:error, :not_eligible}
      )

      assert {:ok, %{node: :accepting@host, warmed_at: ^warmed_at, count: 5}} =
               PeerWarmer.copy_from_peer(Sources.Cache)

      assert_received {:asked, :refusing@host, ^expected_guard}
      assert_received {:asked, :accepting@host, ^expected_guard}
      refute_received {:asked, :unused@host, _guard}
      assert_outcome(ref, Sources.Cache, :copied)
    end

    test "tries the next peer after a failed transfer" do
      stub_attempts(
        down@host: {:error, :peer_down},
        accepting@host: {:ok, 1, %{warmed_at: DateTime.utc_now()}}
      )

      assert {:ok, %{node: :accepting@host}} = PeerWarmer.copy_from_peer(Sources.Cache)
    end

    test "falls back when every peer refuses", %{telemetry_ref: ref} do
      stub_attempts(a@host: {:error, :not_eligible}, b@host: {:error, :not_eligible})

      assert :fallback = PeerWarmer.copy_from_peer(KeyValues.Cache)
      assert_outcome(ref, KeyValues.Cache, :no_eligible_peer)
    end

    test "reports the last transfer failure when no peer could be copied", %{telemetry_ref: ref} do
      stub_attempts(down@host: {:error, :peer_down}, refusing@host: {:error, :not_eligible})

      assert :fallback = PeerWarmer.copy_from_peer(Sources.Cache)
      assert_outcome(ref, Sources.Cache, :peer_down)
    end

    test "shares one copy timeout across all attempts", %{telemetry_ref: ref} do
      Application.put_env(
        :logflare,
        PeerWarmer,
        Keyword.put(Application.fetch_env!(:logflare, PeerWarmer), :copy_timeout, 50)
      )

      stub(ClusterUtils, :peer_list_partial, fn 1.0, _max_nodes -> [:slow@host, :next@host] end)
      test_pid = self()

      stub(Transfer, :run, fn node, _cache, _guard, _import_fun, timeout ->
        send(test_pid, {:asked, node, timeout})
        Process.sleep(60)
        {:error, :timeout}
      end)

      assert :fallback = PeerWarmer.copy_from_peer(Sources.Cache)
      assert_received {:asked, :slow@host, timeout} when timeout <= 50
      refute_received {:asked, :next@host, _timeout}
      assert_outcome(ref, Sources.Cache, :timeout)
    end

    test "falls back when the copy crashes", %{telemetry_ref: ref} do
      stub(ClusterUtils, :peer_list_partial, fn 1.0, _max_nodes -> [:peer@host] end)
      stub(Transfer, :run, fn _node, _cache, _guard, _import_fun, _timeout -> raise "boom" end)

      capture_log(fn ->
        assert :fallback = PeerWarmer.copy_from_peer(Sources.Cache)
      end)

      assert_outcome(ref, Sources.Cache, :crashed)
    end
  end

  describe "source_status/2" do
    test "accepts a ready cache with entries, on the same release" do
      warmed_at = DateTime.utc_now()
      Cachex.put!(Sources.Cache, :key, {:cached, %{id: 1}})
      PeerWarmer.mark_ready(Sources.Cache, warmed_at)

      assert {:ok, %{warmed_at: ^warmed_at}} =
               PeerWarmer.source_status(Sources.Cache, release_version())
    end

    test "refuses before the cache started warming and while it is warming" do
      Cachex.put!(Sources.Cache, :key, {:cached, %{id: 1}})
      assert {:error, :not_eligible} = PeerWarmer.source_status(Sources.Cache, release_version())

      PeerWarmer.mark_warming(Sources.Cache)
      assert {:error, :not_eligible} = PeerWarmer.source_status(Sources.Cache, release_version())
    end

    test "refuses a node on another release" do
      Cachex.put!(Sources.Cache, :key, {:cached, %{id: 1}})
      PeerWarmer.mark_ready(Sources.Cache, DateTime.utc_now())

      assert {:error, :not_eligible} = PeerWarmer.source_status(Sources.Cache, ~c"0.0.0")
    end

    test "refuses an empty cache" do
      PeerWarmer.mark_ready(Sources.Cache, DateTime.utc_now())

      assert {:error, :not_eligible} = PeerWarmer.source_status(Sources.Cache, release_version())
    end

    test "refuses KeyValues data older than the max age" do
      Cachex.put!(KeyValues.Cache, :key, {:cached, %{}})
      PeerWarmer.mark_ready(KeyValues.Cache, DateTime.add(DateTime.utc_now(), -2, :hour))

      assert {:error, :not_eligible} =
               PeerWarmer.source_status(KeyValues.Cache, release_version())
    end

    test "does not apply a max age to Sources" do
      old = DateTime.add(DateTime.utc_now(), -2, :day)
      Cachex.put!(Sources.Cache, :key, {:cached, %{id: 1}})
      PeerWarmer.mark_ready(Sources.Cache, old)

      assert {:ok, %{warmed_at: ^old}} =
               PeerWarmer.source_status(Sources.Cache, release_version())
    end
  end

  describe "copy_from_peer/1 importing" do
    test "imports Sources entries except negative, invalidated and already cached ones" do
      [fresh, invalidated, cached] = insert_list(3, :source, user: insert(:user))
      Gossip.record_tombstones([{Sources, invalidated.id}])
      Cachex.put!(Sources.Cache, get_by_key(cached), {:cached, :local_value})

      stub_transfer_of([
        {get_by_key(fresh), fresh, nil},
        {{:get_by, [[id: -1]]}, nil, nil},
        {get_by_key(invalidated), invalidated, nil},
        {get_by_key(cached), cached, nil}
      ])

      assert {:ok, %{count: 1}} = PeerWarmer.copy_from_peer(Sources.Cache)

      assert {:cached, %{id: fresh_id}} = Cachex.get!(Sources.Cache, get_by_key(fresh))
      assert fresh_id == fresh.id
      assert Cachex.get!(Sources.Cache, get_by_key(invalidated)) == nil
      assert Cachex.get!(Sources.Cache, get_by_key(cached)) == {:cached, :local_value}
    end

    test "keeps the remaining ttl of copied entries" do
      source = insert(:source, user: insert(:user))
      stub_transfer_of([{get_by_key(source), source, to_timeout(minute: 6)}])

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
        {{:lookup, [user_id, "fresh", nil]}, %{"v" => 1}, nil},
        {{:lookup, [user_id, "invalidated", nil]}, %{"v" => 2}, nil},
        {{:lookup, [user_id, "missing", nil]}, nil, nil},
        {{:count, user_id}, 3, nil},
        {{:count, other_user_id}, 4, nil}
      ])

      assert {:ok, %{count: 2}} = PeerWarmer.copy_from_peer(KeyValues.Cache)

      assert {:cached, %{"v" => 1}} =
               Cachex.get!(KeyValues.Cache, {:lookup, [user_id, "fresh", nil]})

      assert {:cached, 4} = Cachex.get!(KeyValues.Cache, {:count, other_user_id})
      refute KeyValues.Cache.cached?({:lookup, [user_id, "invalidated", nil]})
      refute KeyValues.Cache.cached?({:lookup, [user_id, "missing", nil]})
      refute KeyValues.Cache.cached?({:count, user_id})
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

    test "reports the warmed_at, release version and size once ready" do
      warmed_at = DateTime.utc_now()
      version = Application.spec(:logflare, :vsn)
      Cachex.put!(Sources.Cache, :some_key, {:cached, %{id: 1}})
      PeerWarmer.mark_ready(Sources.Cache, warmed_at)

      assert %{state: :ready, warmed_at: ^warmed_at, version: ^version, size: 1} =
               PeerWarmer.status(Sources.Cache)
    end
  end

  defp stub_attempts(attempts) do
    test_pid = self()
    stub(ClusterUtils, :peer_list_partial, fn 1.0, _max_nodes -> Keyword.keys(attempts) end)

    stub(Transfer, :run, fn node, _cache, guard, _import_fun, _timeout ->
      send(test_pid, {:asked, node, guard})
      Keyword.fetch!(attempts, node)
    end)
  end

  defp stub_transfer_of(entries) do
    stub(ClusterUtils, :peer_list_partial, fn 1.0, _max_nodes -> [:peer@host] end)

    stub(Transfer, :run, fn _node, _cache, _guard, import_fun, _timeout ->
      {:ok, import_fun.(entries), %{warmed_at: DateTime.utc_now()}}
    end)
  end

  defp release_version, do: Application.spec(:logflare, :vsn)

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
