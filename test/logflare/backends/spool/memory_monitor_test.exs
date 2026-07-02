defmodule Logflare.Backends.Spool.MemoryMonitorTest do
  use ExUnit.Case, async: false

  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.TestUtils

  setup do
    prev_spool_config = Application.get_env(:logflare, :spool)

    on_exit(fn ->
      if prev_spool_config do
        Application.put_env(:logflare, :spool, prev_spool_config)
      else
        Application.delete_env(:logflare, :spool)
      end
    end)

    :ok
  end

  test "throttled?/0 is true once the configured percent thresholds are exceeded" do
    Application.put_env(:logflare, :spool,
      spool_memory_limit_percent: 0.0,
      spool_max_ets_percent: 0.0
    )

    start_supervised!(MemoryMonitor)
    Process.sleep(50)

    assert MemoryMonitor.throttled?() == true
  end

  test "throttled?/0 is false when comfortably under the configured percent thresholds" do
    Application.put_env(:logflare, :spool,
      spool_memory_limit_percent: 1.0,
      spool_max_ets_percent: 1.0
    )

    start_supervised!(MemoryMonitor)
    Process.sleep(50)

    assert MemoryMonitor.throttled?() == false
  end

  test "refresh/0 emits a [:logflare, :backends, :spool, :throttled] telemetry event on every refresh" do
    TestUtils.attach_forwarder([:logflare, :backends, :spool, :throttled])

    Application.put_env(:logflare, :spool,
      spool_memory_limit_percent: 0.0,
      spool_max_ets_percent: 0.0
    )

    start_supervised!(MemoryMonitor)

    assert_receive {:telemetry_event, [:logflare, :backends, :spool, :throttled],
                    %{throttled: 1, total_percent: total_percent, ets_percent: ets_percent}, %{}},
                   1000

    assert is_float(total_percent)
    assert is_float(ets_percent)
  end
end
