defmodule Logflare.Backends.Spool.MemoryMonitorTest do
  use ExUnit.Case, async: false

  alias Logflare.Backends.Spool.MemoryMonitor

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
end
