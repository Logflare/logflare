defmodule Logflare.ContextCache.RefreshAheadTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.ContextCache
  alias Logflare.ContextCache.RefreshAhead
  alias Logflare.Endpoints
  alias Logflare.Rules

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
    def keys_to_bust(_cache, kw), do: [Keyword.fetch!(kw, :key)]

    @impl Logflare.ContextCache.Ops
    def delete_keys(cache, keys) do
      keys = Enum.to_list(keys)
      notify({:delete_keys, cache, keys})
      {:ok, length(keys)}
    end

    @impl Logflare.ContextCache.Ops
    def fetch(_cache, _key, _getter), do: :stale
    @impl Logflare.ContextCache.Ops
    def update(_cache, _key, _value), do: :ok
    @impl Logflare.ContextCache.Ops
    def entries(_cache), do: []
    @impl Logflare.ContextCache.Ops
    def cached?(_cache, _key), do: true
    @impl Logflare.ContextCache.Ops
    def entry(_cache, key), do: {key, :stale, 1}
    @impl Logflare.ContextCache.Ops
    def expiry(_cache, _key), do: {0, 1_000}
    @impl Logflare.ContextCache.Ops
    def size(_cache), do: 0

    @impl Logflare.ContextCache.Ops
    def put_entries(cache, entries) do
      notify({:put_entries, cache, entries})

      case :persistent_term.get({__MODULE__, :bust_on_write}, nil) do
        nil -> :ok
        key -> {:ok, _busted} = Logflare.ContextCache.bust_keys([{__MODULE__, [key: key]}])
      end

      :ok
    end

    defp notify(message), do: send(:persistent_term.get(__MODULE__), message)
  end

  defmodule FakeOps.Cache do
    use Logflare.ContextCache, impl: FakeOps, refresh_ahead: true
  end

  @event [:logflare, :context_cache, :refresh_ahead]
  @key {:get_rule, [1]}

  setup do
    original_config = Application.fetch_env!(:logflare, RefreshAhead)
    put_config(Keyword.merge(original_config, enabled: true, threshold: 0.5, interval: 10))

    :ets.delete_all_objects(RefreshAhead)
    Rules.Cache.reset()
    Endpoints.Cache.reset()
    :persistent_term.put(FakeOps, self())
    telemetry_ref = :telemetry_test.attach_event_handlers(self(), [@event])

    on_exit(fn ->
      :ets.delete_all_objects(RefreshAhead)
      :persistent_term.erase(FakeOps)
      :persistent_term.erase({FakeOps, :bust_on_write})
      Application.put_env(:logflare, RefreshAhead, original_config)
      :telemetry.detach(telemetry_ref)
    end)

    [telemetry_ref: telemetry_ref]
  end

  test "refreshes an entry close to expiry in the background with the cache's default TTL", %{
    telemetry_ref: ref
  } do
    put_expiring(Rules.Cache, @key, :stale)

    assert :stale = Rules.Cache.fetch(@key, fn -> :fresh end)

    assert_receive {@event, ^ref, _measurements, %{cache: Rules.Cache, result: :refreshed}},
                   1_000

    assert {:fresh, ttl} = cached(Rules.Cache, @key)
    assert ttl > to_timeout(minute: 50)
  end

  test "refreshes entries read through apply_fun/3 from the database", %{telemetry_ref: ref} do
    source = insert(:source, user: insert(:user))
    key = {:list_by_source_id, [source.id]}
    put_expiring(Rules.Cache, key, :stale)

    assert :stale = Rules.Cache.list_by_source_id(source.id)

    assert_receive {@event, ^ref, _measurements, %{cache: Rules.Cache, result: :refreshed}},
                   1_000

    assert {[], _ttl} = cached(Rules.Cache, key)
  end

  test "works with any context cache ops implementation" do
    assert :stale = FakeOps.Cache.fetch(@key, fn -> :fresh end)

    assert_receive {:put_entries, FakeOps.Cache, [{@key, :fresh, nil}]}, 1_000
  end

  test "does not refresh entries with plenty of TTL left", %{telemetry_ref: ref} do
    Rules.Cache.put_entries([{@key, :stale, to_timeout(minute: 1)}])

    assert :stale = Rules.Cache.fetch(@key, fn -> :fresh end)

    refute_receive {@event, ^ref, _measurements, _metadata}, 100
    assert {:stale, _ttl} = cached(Rules.Cache, @key)
  end

  test "does not refresh when disabled", %{telemetry_ref: ref} do
    put_config(enabled: false)
    put_expiring(Rules.Cache, @key, :stale)

    assert :stale = Rules.Cache.fetch(@key, fn -> :fresh end)

    refute_receive {@event, ^ref, _measurements, _metadata}, 100
  end

  test "does not refresh caches that are not opted in", %{telemetry_ref: ref} do
    put_expiring(Endpoints.Cache, @key, :stale)

    assert :stale = Endpoints.Cache.fetch(@key, fn -> :fresh end)

    refute_receive {@event, ^ref, _measurements, _metadata}, 100
  end

  test "queues each key once", %{telemetry_ref: ref} do
    put_expiring(Rules.Cache, @key, :stale)
    getter = blocking_getter(:fresh)

    assert :stale = Rules.Cache.fetch(@key, getter)
    assert_receive {:getter_called, worker}, 1_000
    assert :stale = Rules.Cache.fetch(@key, getter)
    refute_receive {:getter_called, _worker}, 100

    send(worker, :continue)

    assert_receive {@event, ^ref, _measurements, %{result: :refreshed}}, 1_000
    assert {:fresh, _ttl} = cached(Rules.Cache, @key)
  end

  test "drains the queue at a constant rate", %{telemetry_ref: ref} do
    put_config(batch_size: 2, interval: 300)
    keys = for i <- 1..5, do: {:get_rule, [i]}
    Rules.Cache.put_entries(for key <- keys, do: {key, :stale, 2_000})
    Process.sleep(1_100)

    for key <- keys, do: Rules.Cache.fetch(key, fn -> :fresh end)

    assert_receive {@event, ^ref, _measurements, %{result: :refreshed}}, 400
    assert_receive {@event, ^ref, _measurements, %{result: :refreshed}}, 50
    refute_receive {@event, ^ref, _measurements, _metadata}, 200
    assert_receive {@event, ^ref, _measurements, %{result: :refreshed}}, 300
  end

  test "skips a key whose entry was replaced after it was queued", %{telemetry_ref: ref} do
    pause_ticks()
    put_expiring(Rules.Cache, @key, :stale)

    Rules.Cache.fetch(@key, notifying_getter(:fresh))
    Rules.Cache.put_entries([{@key, :warmed, nil}])
    tick_now()

    assert_receive {@event, ^ref, _measurements, %{result: :skipped}}, 1_000
    refute_received :getter_called
    assert {:warmed, _ttl} = cached(Rules.Cache, @key)
  end

  test "skips a key busted while it was queued", %{telemetry_ref: ref} do
    pause_ticks()
    put_expiring(Rules.Cache, @key, :old)

    Rules.Cache.fetch(@key, notifying_getter(:read_before_the_update))
    assert {:ok, 1} = ContextCache.bust_keys([{Rules, 1}])
    put_expiring(Rules.Cache, @key, :new)
    tick_now()

    assert_receive {@event, ^ref, _measurements, %{result: :skipped}}, 1_000
    refute_received :getter_called
    assert {:new, _ttl} = cached(Rules.Cache, @key)
  end

  test "does not restore an entry deleted while refreshing", %{telemetry_ref: ref} do
    put_expiring(Rules.Cache, @key, :stale)

    assert :stale = Rules.Cache.fetch(@key, blocking_getter(:fresh))
    assert_receive {:getter_called, worker}, 1_000
    Rules.Cache.reset()
    send(worker, :continue)

    assert_receive {@event, ^ref, _measurements, %{result: :skipped}}, 1_000
    refute Rules.Cache.cached?(@key)
  end

  test "does not overwrite an entry busted and reloaded while refreshing", %{telemetry_ref: ref} do
    put_expiring(Rules.Cache, @key, :old)

    Rules.Cache.fetch(@key, blocking_getter(:read_before_the_update))
    assert_receive {:getter_called, worker}, 1_000
    assert {:ok, 1} = ContextCache.bust_keys([{Rules, 1}])
    assert :new = Rules.Cache.fetch(@key, fn -> :new end)
    send(worker, :continue)

    assert_receive {@event, ^ref, _measurements, %{result: :skipped}}, 1_000
    assert {:new, _ttl} = cached(Rules.Cache, @key)
  end

  test "evicts its write when the key is busted between the check and the write", %{
    telemetry_ref: ref
  } do
    :persistent_term.put({FakeOps, :bust_on_write}, @key)

    FakeOps.Cache.fetch(@key, fn -> :fresh end)

    assert_receive {@event, ^ref, _measurements, %{cache: FakeOps.Cache, result: :skipped}}, 1_000
    assert_received {:put_entries, FakeOps.Cache, [{@key, :fresh, nil}]}
    assert_received {:delete_keys, FakeOps.Cache, [@key]}
    assert_received {:delete_keys, FakeOps.Cache, [@key]}
  end

  test "keeps its write when another key is busted meanwhile", %{telemetry_ref: ref} do
    :persistent_term.put({FakeOps, :bust_on_write}, :other_key)

    FakeOps.Cache.fetch(@key, fn -> :fresh end)

    assert_receive {@event, ^ref, _measurements, %{cache: FakeOps.Cache, result: :refreshed}},
                   1_000

    assert_received {:delete_keys, FakeOps.Cache, [:other_key]}
    refute_received {:delete_keys, FakeOps.Cache, [@key]}
  end

  test "keeps the cached value when the getter raises", %{telemetry_ref: ref} do
    put_expiring(Rules.Cache, @key, :stale)

    capture_log(fn ->
      assert :stale = Rules.Cache.fetch(@key, fn -> raise "db down" end)
      assert_receive {@event, ^ref, _measurements, %{result: :failed}}, 1_000
    end)

    assert {:stale, _ttl} = cached(Rules.Cache, @key)
    assert :stale = Rules.Cache.fetch(@key, fn -> :fresh end)
    assert_receive {@event, ^ref, _measurements, %{result: :refreshed}}, 1_000
  end

  test "keeps the cached value when the database is unavailable", %{telemetry_ref: ref} do
    put_expiring(Rules.Cache, @key, :stale)

    capture_log(fn ->
      assert :stale = Rules.Cache.fetch(@key, fn -> {:error, :database_unavailable} end)
      assert_receive {@event, ^ref, _measurements, %{result: :failed}}, 1_000
    end)

    assert {:stale, _ttl} = cached(Rules.Cache, @key)
  end

  defp put_config(config) do
    current = Application.fetch_env!(:logflare, RefreshAhead)
    Application.put_env(:logflare, RefreshAhead, Keyword.merge(current, config))
  end

  defp pause_ticks do
    put_config(interval: to_timeout(minute: 1))
    Process.sleep(50)
    on_exit(fn -> resume_ticks() end)
  end

  defp resume_ticks do
    put_config(interval: 10)
    tick_now()
  end

  defp tick_now, do: send(Process.whereis(RefreshAhead), :tick)

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

  defp notifying_getter(value) do
    test_pid = self()

    fn ->
      send(test_pid, :getter_called)
      value
    end
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
