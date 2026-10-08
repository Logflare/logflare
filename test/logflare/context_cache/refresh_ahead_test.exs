defmodule Logflare.ContextCache.RefreshAheadTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache.RefreshAhead
  alias Logflare.Endpoints
  alias Logflare.Sources
  alias Logflare.Sources.Source

  defmodule FakeOps do
    @behaviour Logflare.Cache.Ops
    @behaviour Logflare.ContextCache.Ops

    @impl Logflare.Cache.Ops
    def healthy?(_cache), do: true
    @impl Logflare.Cache.Ops
    def stats(_cache), do: %{}
    @impl Logflare.Cache.Ops
    def reset(_cache), do: :ok

    @impl Logflare.ContextCache.Ops
    def bust_by(_cache, _kw), do: {:ok, 0}
    @impl Logflare.ContextCache.Ops
    def fetch(_cache, _key, _getter), do: :stale
    @impl Logflare.ContextCache.Ops
    def update(_cache, _key, _value), do: :ok
    @impl Logflare.ContextCache.Ops
    def entries(_cache), do: []
    @impl Logflare.ContextCache.Ops
    def cached?(_cache, _key), do: true
    @impl Logflare.ContextCache.Ops
    def entry(_cache, _key), do: nil
    @impl Logflare.ContextCache.Ops
    def expiry(_cache, _key), do: {0, 1_000}
    @impl Logflare.ContextCache.Ops
    def size(_cache), do: 0

    @impl Logflare.ContextCache.Ops
    def put_entries(cache, entries) do
      send(:persistent_term.get(__MODULE__), {:put_entries, cache, entries})
      :ok
    end
  end

  defmodule FakeOps.Cache do
    use Logflare.ContextCache, impl: FakeOps, refresh_ahead: true
  end

  @event [:logflare, :context_cache, :refresh_ahead]
  @key {:refresh_ahead_test, [1]}

  setup do
    original_config = Application.fetch_env!(:logflare, RefreshAhead)
    put_config(Keyword.merge(original_config, enabled: true, threshold: 0.5, interval: 10))

    stub(ClusterUtils, :peer_list_partial, fn _ratio, _max_nodes -> [] end)

    :ets.delete_all_objects(RefreshAhead)
    Sources.Cache.reset()
    Endpoints.Cache.reset()
    telemetry_ref = :telemetry_test.attach_event_handlers(self(), [@event])

    on_exit(fn ->
      :ets.delete_all_objects(RefreshAhead)
      Application.put_env(:logflare, RefreshAhead, original_config)
      :telemetry.detach(telemetry_ref)
    end)

    [telemetry_ref: telemetry_ref]
  end

  test "refreshes an entry close to expiry in the background and returns the cached value", %{
    telemetry_ref: ref
  } do
    put_expiring(Sources.Cache, @key, :stale)

    assert :stale = Sources.Cache.fetch(@key, fn -> :fresh end)

    assert_receive {@event, ^ref, _measurements, %{cache: Sources.Cache, result: :refreshed}},
                   1_000

    assert {:fresh, ttl} = cached(Sources.Cache, @key)
    assert ttl > 600
  end

  test "refreshes entries read through apply_fun/3 from the database", %{telemetry_ref: ref} do
    insert(:plan, name: "Free")
    source = insert(:source, user: insert(:user))
    key = {:get_by, [[token: source.token]]}
    put_expiring(Sources.Cache, key, %{source | name: "stale"})

    assert %Source{name: "stale"} = Sources.Cache.get_by(token: source.token)

    assert_receive {@event, ^ref, _measurements, %{cache: Sources.Cache, result: :refreshed}},
                   1_000

    assert {%Source{name: name}, _ttl} = cached(Sources.Cache, key)
    assert name == source.name
  end

  test "works with any context cache ops implementation" do
    :persistent_term.put(FakeOps, self())
    on_exit(fn -> :persistent_term.erase(FakeOps) end)

    assert :stale = FakeOps.Cache.fetch(@key, fn -> :fresh end)

    assert_receive {:put_entries, FakeOps.Cache, [{@key, :fresh, 1_000}]}, 1_000
  end

  test "does not refresh entries with plenty of TTL left", %{telemetry_ref: ref} do
    Sources.Cache.put_entries([{@key, :stale, to_timeout(minute: 1)}])

    assert :stale = Sources.Cache.fetch(@key, fn -> :fresh end)

    refute_receive {@event, ^ref, _measurements, _metadata}, 100
    assert {:stale, _ttl} = cached(Sources.Cache, @key)
  end

  test "does not refresh when disabled", %{telemetry_ref: ref} do
    put_config(enabled: false)
    put_expiring(Sources.Cache, @key, :stale)

    assert :stale = Sources.Cache.fetch(@key, fn -> :fresh end)

    refute_receive {@event, ^ref, _measurements, _metadata}, 100
  end

  test "does not refresh caches that are not opted in", %{telemetry_ref: ref} do
    put_expiring(Endpoints.Cache, @key, :stale)

    assert :stale = Endpoints.Cache.fetch(@key, fn -> :fresh end)

    refute_receive {@event, ^ref, _measurements, _metadata}, 100
  end

  test "queues each key once", %{telemetry_ref: ref} do
    put_expiring(Sources.Cache, @key, :stale)
    getter = blocking_getter(:fresh)

    assert :stale = Sources.Cache.fetch(@key, getter)
    assert_receive {:getter_called, worker}, 1_000
    assert :stale = Sources.Cache.fetch(@key, getter)
    refute_receive {:getter_called, _worker}, 100

    send(worker, :continue)

    assert_receive {@event, ^ref, _measurements, %{result: :refreshed}}, 1_000
    assert {:fresh, _ttl} = cached(Sources.Cache, @key)
  end

  test "drains the queue at a constant rate", %{telemetry_ref: ref} do
    put_config(batch_size: 2, interval: 300)
    keys = for i <- 1..5, do: {:refresh_ahead_test, [i]}
    Sources.Cache.put_entries(for key <- keys, do: {key, :stale, 2_000})
    Process.sleep(1_100)

    for key <- keys, do: Sources.Cache.fetch(key, fn -> :fresh end)

    assert_receive {@event, ^ref, _measurements, %{result: :refreshed}}, 400
    assert_receive {@event, ^ref, _measurements, %{result: :refreshed}}, 50
    refute_receive {@event, ^ref, _measurements, _metadata}, 200
    assert_receive {@event, ^ref, _measurements, %{result: :refreshed}}, 300
  end

  test "does not restore an entry deleted while refreshing", %{telemetry_ref: ref} do
    put_expiring(Sources.Cache, @key, :stale)

    assert :stale = Sources.Cache.fetch(@key, blocking_getter(:fresh))
    assert_receive {:getter_called, worker}, 1_000
    Sources.Cache.reset()
    send(worker, :continue)

    assert_receive {@event, ^ref, _measurements, %{result: :skipped}}, 1_000
    refute Sources.Cache.cached?(@key)
  end

  test "keeps the cached value when the refresh fails", %{telemetry_ref: ref} do
    put_expiring(Sources.Cache, @key, :stale)

    capture_log(fn ->
      assert :stale = Sources.Cache.fetch(@key, fn -> raise "db down" end)
      assert_receive {@event, ^ref, _measurements, %{result: :failed}}, 1_000
    end)

    assert {:stale, _ttl} = cached(Sources.Cache, @key)
    assert :stale = Sources.Cache.fetch(@key, fn -> :fresh end)
    assert_receive {@event, ^ref, _measurements, %{result: :refreshed}}, 1_000
  end

  test "reloads from the getter when peers can't be reached", %{telemetry_ref: ref} do
    stub(ClusterUtils, :peer_list_partial, fn _ratio, _max_nodes -> [:"nobody@127.0.0.1"] end)
    put_expiring(Sources.Cache, @key, :stale)

    assert :stale = Sources.Cache.fetch(@key, fn -> :fresh end)

    assert_receive {@event, ^ref, _measurements, %{result: :refreshed}}, 1_000
    assert {:fresh, _ttl} = cached(Sources.Cache, @key)
  end

  test "peer_entries/2 returns only entries cached on this node" do
    Sources.Cache.put_entries([{@key, :value, to_timeout(minute: 1)}])

    assert [{@key, :value, ttl}] = RefreshAhead.peer_entries(Sources.Cache, [@key, :missing])
    assert ttl > to_timeout(second: 50)
  end

  defp put_config(config) do
    current = Application.fetch_env!(:logflare, RefreshAhead)
    Application.put_env(:logflare, RefreshAhead, Keyword.merge(current, config))
  end

  defp put_expiring(cache, key, value) do
    cache.put_entries([{key, value, 1_000}])
    Process.sleep(600)
  end

  defp cached(cache, key) do
    Enum.find_value(cache.entries(), fn
      {^key, value, ttl} -> {value, ttl}
      _other -> nil
    end)
  end

  defp blocking_getter(value) do
    test_pid = self()

    fn ->
      send(test_pid, {:getter_called, self()})

      receive do
        :continue -> value
      end
    end
  end
end
