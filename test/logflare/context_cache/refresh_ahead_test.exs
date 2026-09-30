defmodule Logflare.ContextCache.RefreshAheadTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.ContextCache
  alias Logflare.ContextCache.RefreshAhead
  alias Logflare.Endpoints
  alias Logflare.Sources
  alias Logflare.Sources.Source

  @stop_event [:logflare, :context_cache, :refresh_ahead, :stop]
  @key {:refresh_ahead_test, [1]}

  setup do
    original_config = Application.fetch_env!(:logflare, RefreshAhead)

    Application.put_env(
      :logflare,
      RefreshAhead,
      Keyword.merge(original_config, enabled: true, threshold: 0.5)
    )

    Cachex.clear!(Sources.Cache)
    Cachex.clear!(Endpoints.Cache)
    telemetry_ref = :telemetry_test.attach_event_handlers(self(), [@stop_event])

    on_exit(fn ->
      Application.put_env(:logflare, RefreshAhead, original_config)
      :telemetry.detach(telemetry_ref)
    end)

    [telemetry_ref: telemetry_ref]
  end

  test "refreshes an entry close to expiry in the background and returns the cached value", %{
    telemetry_ref: ref
  } do
    put_expiring(Sources.Cache, @key, :stale)

    assert :stale = ContextCache.fetch(Sources.Cache, @key, fn -> :fresh end)

    assert_receive {@stop_event, ^ref, _measurements, %{cache: Sources.Cache, result: :refreshed}}
    assert {:ok, {:cached, :fresh}} = Cachex.get(Sources.Cache, @key)
    assert {:ok, ttl} = Cachex.ttl(Sources.Cache, @key)
    assert ttl > 1_000
  end

  test "refreshes entries read through apply_fun/3 from the database", %{telemetry_ref: ref} do
    insert(:plan, name: "Free")
    source = insert(:source, user: insert(:user))
    key = {:get_by, [[token: source.token]]}
    put_expiring(Sources.Cache, key, %{source | name: "stale"})

    assert %Source{name: "stale"} = Sources.Cache.get_by(token: source.token)

    assert_receive {@stop_event, ^ref, _measurements, %{cache: Sources.Cache, result: :refreshed}}
    assert {:ok, {:cached, %Source{name: name}}} = Cachex.get(Sources.Cache, key)
    assert name == source.name
  end

  test "does not refresh entries with plenty of TTL left", %{telemetry_ref: ref} do
    Cachex.put(Sources.Cache, @key, {:cached, :stale}, expire: :timer.minutes(1))

    assert :stale = ContextCache.fetch(Sources.Cache, @key, fn -> :fresh end)

    refute_receive {@stop_event, ^ref, _measurements, _metadata}, 100
    assert {:ok, {:cached, :stale}} = Cachex.get(Sources.Cache, @key)
  end

  test "does not refresh when disabled", %{telemetry_ref: ref} do
    config = Application.fetch_env!(:logflare, RefreshAhead)
    Application.put_env(:logflare, RefreshAhead, Keyword.put(config, :enabled, false))
    put_expiring(Sources.Cache, @key, :stale)

    assert :stale = ContextCache.fetch(Sources.Cache, @key, fn -> :fresh end)

    refute_receive {@stop_event, ^ref, _measurements, _metadata}, 100
  end

  test "does not refresh caches that are not opted in", %{telemetry_ref: ref} do
    refute Endpoints.Cache in RefreshAhead.caches()
    put_expiring(Endpoints.Cache, @key, :stale)

    assert :stale = ContextCache.fetch(Endpoints.Cache, @key, fn -> :fresh end)

    refute_receive {@stop_event, ^ref, _measurements, _metadata}, 100
  end

  test "runs a single refresh per key at a time", %{telemetry_ref: ref} do
    put_expiring(Sources.Cache, @key, :stale)
    getter = blocking_getter(:fresh)

    assert :stale = ContextCache.fetch(Sources.Cache, @key, getter)
    assert_receive {:getter_called, task}
    assert :stale = ContextCache.fetch(Sources.Cache, @key, getter)
    refute_receive {:getter_called, _task}, 100

    send(task, :continue)

    assert_receive {@stop_event, ^ref, _measurements, %{result: :refreshed}}
    assert {:ok, {:cached, :fresh}} = Cachex.get(Sources.Cache, @key)
  end

  test "does not restore an entry deleted while refreshing", %{telemetry_ref: ref} do
    put_expiring(Sources.Cache, @key, :stale)

    assert :stale = ContextCache.fetch(Sources.Cache, @key, blocking_getter(:fresh))
    assert_receive {:getter_called, task}
    Cachex.del(Sources.Cache, @key)
    send(task, :continue)

    assert_receive {@stop_event, ^ref, _measurements, %{result: :skipped}}
    assert {:ok, nil} = Cachex.get(Sources.Cache, @key)
  end

  test "keeps the cached value when the refresh fails", %{telemetry_ref: ref} do
    put_expiring(Sources.Cache, @key, :stale)

    capture_log(fn ->
      assert :stale = ContextCache.fetch(Sources.Cache, @key, fn -> raise "db down" end)
      assert_receive {@stop_event, ^ref, _measurements, %{result: :failed}}
    end)

    assert {:ok, {:cached, :stale}} = Cachex.get(Sources.Cache, @key)
    assert :stale = ContextCache.fetch(Sources.Cache, @key, fn -> :fresh end)
    assert_receive {@stop_event, ^ref, _measurements, %{result: :refreshed}}
  end

  defp put_expiring(cache, key, value) do
    Cachex.put(cache, key, {:cached, value}, expire: 1_000)
    Process.sleep(600)
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
