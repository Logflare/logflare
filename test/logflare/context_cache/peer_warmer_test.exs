defmodule Logflare.ContextCache.PeerWarmerTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache
  alias Logflare.ContextCache.Gossip
  alias Logflare.ContextCache.PeerWarmer
  alias Logflare.ContextCache.PeerWarmer.Transfer
  alias Logflare.ContextCache.Tombstones
  alias Logflare.KeyValues
  alias Logflare.Sources

  @stop_event [:logflare, :context_cache, :peer_warm, :stop]
  @user_id 1

  setup do
    original_config = Application.fetch_env!(:logflare, PeerWarmer)

    Application.put_env(
      :logflare,
      PeerWarmer,
      Keyword.merge(original_config, enabled: true, peer_wait: 10)
    )

    Cachex.clear!(KeyValues.Cache)
    Cachex.clear!(Tombstones.Cache)
    erase_status()

    telemetry_ref = :telemetry_test.attach_event_handlers(self(), [@stop_event])

    on_exit(fn ->
      Application.put_env(:logflare, PeerWarmer, original_config)
      :telemetry.detach(telemetry_ref)
      erase_status()
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

      assert :fallback = PeerWarmer.copy_from_peer(KeyValues.Cache)
    end

    test "falls back when there are no peers", %{telemetry_ref: ref} do
      stub(ClusterUtils, :peer_list_partial, fn _ratio, _max_nodes -> [] end)

      assert :fallback = PeerWarmer.copy_from_peer(KeyValues.Cache)
      assert_outcome(ref, :no_peers)
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

      assert :fallback = PeerWarmer.copy_from_peer(KeyValues.Cache)
      assert_outcome(ref, :cache_buster_not_started)
    end

    test "asks peers one after another and copies from the first that accepts", %{
      telemetry_ref: ref
    } do
      warmed_at = DateTime.add(DateTime.utc_now(), -10, :minute)
      expected_guard = {PeerWarmer, :source_status, [KeyValues.Cache, release_version()]}

      stub_attempts(
        refusing@host: {:error, :not_eligible},
        accepting@host: {:ok, 5, %{warmed_at: warmed_at}},
        unused@host: {:error, :not_eligible}
      )

      assert {:ok, %{node: :accepting@host, warmed_at: ^warmed_at, count: 5}} =
               PeerWarmer.copy_from_peer(KeyValues.Cache)

      assert_received {:asked, :refusing@host, ^expected_guard}
      assert_received {:asked, :accepting@host, ^expected_guard}
      refute_received {:asked, :unused@host, _guard}
      assert_outcome(ref, :copied)
    end

    test "tries the next peer after a failed transfer" do
      stub_attempts(
        down@host: {:error, :peer_down},
        accepting@host: {:ok, 1, %{warmed_at: DateTime.utc_now()}}
      )

      assert {:ok, %{node: :accepting@host}} = PeerWarmer.copy_from_peer(KeyValues.Cache)
    end

    test "falls back when every peer refuses", %{telemetry_ref: ref} do
      stub_attempts(a@host: {:error, :not_eligible}, b@host: {:error, :not_eligible})

      assert :fallback = PeerWarmer.copy_from_peer(KeyValues.Cache)
      assert_outcome(ref, :no_eligible_peer)
    end

    test "reports the last transfer failure when no peer could be copied", %{telemetry_ref: ref} do
      stub_attempts(down@host: {:error, :peer_down}, refusing@host: {:error, :not_eligible})

      assert :fallback = PeerWarmer.copy_from_peer(KeyValues.Cache)
      assert_outcome(ref, :peer_down)
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

      assert :fallback = PeerWarmer.copy_from_peer(KeyValues.Cache)
      assert_received {:asked, :slow@host, timeout} when timeout <= 50
      refute_received {:asked, :next@host, _timeout}
      assert_outcome(ref, :timeout)
    end

    test "tries the next peer when a transfer copies no entries" do
      stub_attempts(
        empty@host: {:ok, 0, %{warmed_at: DateTime.utc_now()}},
        accepting@host: {:ok, 3, %{warmed_at: DateTime.utc_now()}}
      )

      assert {:ok, %{node: :accepting@host, count: 3}} =
               PeerWarmer.copy_from_peer(KeyValues.Cache)
    end

    test "falls back when no transfer copies any entries", %{telemetry_ref: ref} do
      stub_attempts(
        a@host: {:ok, 0, %{warmed_at: DateTime.utc_now()}},
        b@host: {:ok, 0, %{warmed_at: DateTime.utc_now()}}
      )

      assert :fallback = PeerWarmer.copy_from_peer(KeyValues.Cache)
      assert_outcome(ref, :no_entries)
    end

    test "falls back when the copy crashes", %{telemetry_ref: ref} do
      stub(ClusterUtils, :peer_list_partial, fn 1.0, _max_nodes -> [:peer@host] end)
      stub(Transfer, :run, fn _node, _cache, _guard, _import_fun, _timeout -> raise "boom" end)

      capture_log(fn ->
        assert :fallback = PeerWarmer.copy_from_peer(KeyValues.Cache)
      end)

      assert_outcome(ref, :crashed)
    end
  end

  describe "source_status/2" do
    test "accepts a ready cache with entries, on the same release" do
      warmed_at = DateTime.utc_now()
      put_cached(lookup("key"), %{"v" => 1})
      PeerWarmer.mark_ready(KeyValues.Cache, warmed_at)

      assert {:ok, %{warmed_at: ^warmed_at}} =
               PeerWarmer.source_status(KeyValues.Cache, release_version())
    end

    test "refuses before the cache started warming and while it is warming" do
      put_cached(lookup("key"), %{"v" => 1})

      assert {:error, :not_eligible} =
               PeerWarmer.source_status(KeyValues.Cache, release_version())

      PeerWarmer.mark_warming(KeyValues.Cache)

      assert {:error, :not_eligible} =
               PeerWarmer.source_status(KeyValues.Cache, release_version())
    end

    test "refuses a node on another release" do
      put_cached(lookup("key"), %{"v" => 1})
      PeerWarmer.mark_ready(KeyValues.Cache, DateTime.utc_now())

      assert {:error, :not_eligible} = PeerWarmer.source_status(KeyValues.Cache, ~c"0.0.0")
    end

    test "refuses an empty cache" do
      PeerWarmer.mark_ready(KeyValues.Cache, DateTime.utc_now())

      assert {:error, :not_eligible} =
               PeerWarmer.source_status(KeyValues.Cache, release_version())
    end

    test "refuses a cache holding only expired entries" do
      Cachex.put(KeyValues.Cache, lookup("key"), {:cached, %{"v" => 1}}, expire: 1)
      Process.sleep(5)
      PeerWarmer.mark_ready(KeyValues.Cache, DateTime.utc_now())

      assert {:error, :not_eligible} =
               PeerWarmer.source_status(KeyValues.Cache, release_version())
    end

    test "refuses data older than the max age" do
      put_cached(lookup("key"), %{"v" => 1})
      PeerWarmer.mark_ready(KeyValues.Cache, DateTime.add(DateTime.utc_now(), -2, :hour))

      assert {:error, :not_eligible} =
               PeerWarmer.source_status(KeyValues.Cache, release_version())
    end
  end

  describe "copy_from_peer/1 importing across attempts" do
    test "discards what a failed attempt wrote, so the next peer's values are kept" do
      stub(ClusterUtils, :peer_list_partial, fn 1.0, _max_nodes -> [:failing@host, :next@host] end)

      stub(Transfer, :run, fn
        :failing@host, _cache, _guard, import_fun, _timeout ->
          import_fun.([
            {lookup("shared"), %{"from" => "failing"}, nil},
            {lookup("failed_only"), %{"v" => 1}, nil}
          ])

          {:error, :timeout}

        :next@host, _cache, _guard, import_fun, _timeout ->
          count = import_fun.([{lookup("shared"), %{"from" => "next"}, nil}])
          {:ok, count, %{warmed_at: DateTime.utc_now()}}
      end)

      assert {:ok, %{node: :next@host, count: 1}} = PeerWarmer.copy_from_peer(KeyValues.Cache)
      assert Cachex.get!(KeyValues.Cache, lookup("shared")) == {:cached, %{"from" => "next"}}
      refute KeyValues.Cache.cached?(lookup("failed_only"))
    end

    test "discards what a failed attempt wrote before falling back to the database" do
      stub(ClusterUtils, :peer_list_partial, fn 1.0, _max_nodes -> [:failing@host] end)

      stub(Transfer, :run, fn _node, _cache, _guard, import_fun, _timeout ->
        import_fun.([{lookup("written"), %{"v" => 1}, nil}])
        {:error, :peer_down}
      end)

      assert :fallback = PeerWarmer.copy_from_peer(KeyValues.Cache)
      refute KeyValues.Cache.cached?(lookup("written"))
    end

    test "keeps entries that were cached locally before a failed attempt" do
      put_cached(lookup("local"), %{"from" => "local"})
      stub(ClusterUtils, :peer_list_partial, fn 1.0, _max_nodes -> [:failing@host] end)

      stub(Transfer, :run, fn _node, _cache, _guard, import_fun, _timeout ->
        import_fun.([{lookup("local"), %{"from" => "peer"}, nil}])
        {:error, :peer_down}
      end)

      assert :fallback = PeerWarmer.copy_from_peer(KeyValues.Cache)
      assert Cachex.get!(KeyValues.Cache, lookup("local")) == {:cached, %{"from" => "local"}}
    end
  end

  describe "copy_from_peer/1 importing" do
    test "imports entries except negative, invalidated and already cached ones" do
      other_user_id = 2
      Gossip.record_tombstones([{KeyValues, [user_id: @user_id, key: "invalidated"]}])
      put_cached(lookup("cached"), %{"from" => "local"})

      stub_transfer_of([
        {lookup("fresh"), %{"v" => 1}, nil},
        {lookup("invalidated"), %{"v" => 2}, nil},
        {lookup("missing"), nil, nil},
        {lookup("cached"), %{"from" => "peer"}, nil},
        {{:count, @user_id}, 3, nil},
        {{:count, other_user_id}, 4, nil}
      ])

      assert {:ok, %{count: 2}} = PeerWarmer.copy_from_peer(KeyValues.Cache)

      assert Cachex.get!(KeyValues.Cache, lookup("fresh")) == {:cached, %{"v" => 1}}
      assert Cachex.get!(KeyValues.Cache, {:count, other_user_id}) == {:cached, 4}
      assert Cachex.get!(KeyValues.Cache, lookup("cached")) == {:cached, %{"from" => "local"}}
      refute KeyValues.Cache.cached?(lookup("invalidated"))
      refute KeyValues.Cache.cached?(lookup("missing"))
      refute KeyValues.Cache.cached?({:count, @user_id})
    end

    test "does not keep an entry invalidated between the stale check and the write" do
      stub_transfer_of([
        {lookup("invalidated"), %{"v" => 1}, nil},
        {lookup("unchanged"), %{"v" => 2}, nil}
      ])

      stub(KeyValues.Cache, :put_entries, fn entries ->
        change = [{KeyValues, [user_id: @user_id, key: "invalidated"]}]
        Gossip.record_tombstones(change)
        ContextCache.bust_keys(change)
        call_original(KeyValues.Cache, :put_entries, [entries])
      end)

      result = PeerWarmer.copy_from_peer(KeyValues.Cache)

      refute KeyValues.Cache.cached?(lookup("invalidated"))
      assert Cachex.get!(KeyValues.Cache, lookup("unchanged")) == {:cached, %{"v" => 2}}
      assert {:ok, %{count: 1}} = result
    end

    test "keeps the remaining ttl of copied entries" do
      stub_transfer_of([{lookup("key"), %{"v" => 1}, to_timeout(minute: 6)}])

      assert {:ok, %{count: 1}} = PeerWarmer.copy_from_peer(KeyValues.Cache)

      assert {:ok, remaining} = Cachex.ttl(KeyValues.Cache, lookup("key"))
      assert remaining <= to_timeout(minute: 6)
      assert remaining > to_timeout(minute: 5)
    end
  end

  describe "status/1" do
    test "is nil until the cache has started warming" do
      assert PeerWarmer.status(KeyValues.Cache) == nil
    end

    test "is nil for caches that are not copied from peers" do
      assert PeerWarmer.status(Sources.Cache) == nil
      assert PeerWarmer.status(Logflare.Users.Cache) == nil
    end

    test "reports the warming state" do
      PeerWarmer.mark_warming(KeyValues.Cache)

      assert %{state: :warming, warmed_at: nil} = PeerWarmer.status(KeyValues.Cache)
    end

    test "reports the warmed_at, release version and size once ready" do
      warmed_at = DateTime.utc_now()
      version = Application.spec(:logflare, :vsn)
      put_cached(lookup("key"), %{"v" => 1})
      PeerWarmer.mark_ready(KeyValues.Cache, warmed_at)

      assert %{state: :ready, warmed_at: ^warmed_at, version: ^version, size: 1} =
               PeerWarmer.status(KeyValues.Cache)
    end
  end

  test "Sources are not copied from peers" do
    assert_raise FunctionClauseError, fn -> PeerWarmer.copy_from_peer(Sources.Cache) end
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

  defp lookup(key), do: {:lookup, [@user_id, key, nil]}

  defp put_cached(key, value), do: Cachex.put!(KeyValues.Cache, key, {:cached, value})

  defp release_version, do: Application.spec(:logflare, :vsn)

  defp assert_outcome(ref, outcome) do
    assert_received {@stop_event, ^ref, _measurements,
                     %{cache: KeyValues.Cache, outcome: ^outcome}}
  end

  defp erase_status, do: :persistent_term.erase({PeerWarmer, KeyValues.Cache})
end
