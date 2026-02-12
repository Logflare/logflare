defmodule Logflare.SystemMetrics.CachexStatsTest do
  use Logflare.DataCase, async: false
  alias Logflare.Users.Cache, as: UsersCache
  alias Logflare.Sources.Cache, as: SourcesCache

  test "cachex_metrics/0 iterates caches and emits telemetry events" do
    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:cachex, :users],
        [:cachex, :sources]
      ])

    on_exit(fn ->
      :telemetry.detach(telemetry_ref)
      Cachex.clear(UsersCache)
      Cachex.clear(SourcesCache)
    end)

    Cachex.put(UsersCache, "u1", "v1")
    Cachex.get(UsersCache, "u1")
    Cachex.get(UsersCache, "missing")

    Cachex.put(SourcesCache, "s1", "v1")
    Cachex.get(SourcesCache, "s1")

    # initate telemetry_poller tick
    Logflare.Telemetry.cachex_metrics()

    assert_received {[:cachex, :users], ^telemetry_ref, users_cache_measurements, _metadata}

    assert Map.keys(users_cache_measurements) == [
             :total_heap_size,
             :purge,
             :stats,
             :operations,
             :hits,
             :evictions,
             :expirations,
             :misses,
             :hit_rate,
             :miss_rate
           ]

    assert users_cache_measurements.hits >= 1
    assert users_cache_measurements.misses >= 1
    assert users_cache_measurements.operations >= 3

    assert_received {[:cachex, :sources], ^telemetry_ref, sources_cache_measurements, _metadata}

    assert Map.keys(sources_cache_measurements) == [
             :total_heap_size,
             :purge,
             :stats,
             :operations,
             :hits,
             :evictions,
             :expirations,
             :misses,
             :hit_rate,
             :miss_rate
           ]

    assert sources_cache_measurements.hits >= 1
    assert sources_cache_measurements.misses >= 0
    assert sources_cache_measurements.operations >= 2
  end
end
