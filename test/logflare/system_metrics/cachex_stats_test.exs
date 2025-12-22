defmodule Logflare.SystemMetrics.CachexStatsTest do
  use Logflare.DataCase, async: false
  alias Logflare.Users.Cache, as: UsersCache
  alias Logflare.Sources.Cache, as: SourcesCache

  test "cachex_metrics/0 iterates caches and emits telemetry events", %{test: test} do
    # Cleanup handler on exit
    handler_id = "#{test}-handler"

    on_exit(fn ->
      :telemetry.detach(handler_id)
      # Clear caches
      Cachex.clear(UsersCache)
      Cachex.clear(SourcesCache)
    end)

    # 1. Setup Data
    # Generate hits/misses for UsersCache
    Cachex.put(UsersCache, "u1", "v1")
    # Hit
    Cachex.get(UsersCache, "u1")
    # Miss
    Cachex.get(UsersCache, "missing")

    # Generate hits for SourcesCache
    Cachex.put(SourcesCache, "s1", "v1")
    # Hit
    Cachex.get(SourcesCache, "s1")

    # 2. Attach Listener
    test_pid = self()

    :telemetry.attach_many(
      handler_id,
      [
        [:cachex, :users],
        [:cachex, :sources]
      ],
      fn event_name, measurements, _metadata, _config ->
        send(test_pid, {:telemetry_event, event_name, measurements})
      end,
      nil
    )

    # 3. Trigger Metrics Collection (simulate poller tick)
    Logflare.Telemetry.cachex_metrics()

    # 4. Assertions
    # Expect Users Event
    assert_receive {:telemetry_event, [:cachex, :users], measurements}
    assert measurements.hits >= 1
    assert measurements.misses >= 1
    assert measurements.hit_rate == 50.0

    # Expect Sources Event
    assert_receive {:telemetry_event, [:cachex, :sources], measurements}
    assert measurements.hits >= 1
    assert measurements.misses == 0
    assert measurements.hit_rate == 100.0
  end
end
