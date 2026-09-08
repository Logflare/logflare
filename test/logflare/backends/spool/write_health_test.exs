defmodule Logflare.Backends.Spool.WriteHealthTest do
  # Global, process-independent state (mirrors Logflare.Readiness) — async:
  # false so this never overlaps with any other test reading/writing it.
  use ExUnit.Case, async: false

  alias Logflare.Backends.Spool.WriteHealth

  setup do
    prev_spool_config = Application.get_env(:logflare, :spool)

    on_exit(fn ->
      WriteHealth.report_recovery!()

      if prev_spool_config do
        Application.put_env(:logflare, :spool, prev_spool_config)
      else
        Application.delete_env(:logflare, :spool)
      end
    end)

    :ok
  end

  test "healthy?/0 is true by default" do
    assert WriteHealth.healthy?() == true
  end

  test "report_failure!/0 marks it unhealthy once max_write_health_failures is reached" do
    Application.put_env(:logflare, :spool, max_write_health_failures: 3)

    WriteHealth.report_failure!()
    assert WriteHealth.healthy?() == true

    WriteHealth.report_failure!()
    assert WriteHealth.healthy?() == true

    WriteHealth.report_failure!()
    assert WriteHealth.healthy?() == false
  end

  test "report_recovery!/0 resets the failure count, not just the healthy flag" do
    Application.put_env(:logflare, :spool, max_write_health_failures: 3)

    WriteHealth.report_failure!()
    WriteHealth.report_failure!()
    WriteHealth.report_recovery!()

    # If the count hadn't reset, one more failure would already reach 3 and
    # flip it unhealthy — proving it takes another full run of 3 shows the
    # counter itself was cleared, not just the healthy flag.
    WriteHealth.report_failure!()
    assert WriteHealth.healthy?() == true

    WriteHealth.report_failure!()
    assert WriteHealth.healthy?() == true

    WriteHealth.report_failure!()
    assert WriteHealth.healthy?() == false
  end

  test "report_recovery!/0 clears an unhealthy state" do
    Application.put_env(:logflare, :spool, max_write_health_failures: 1)
    WriteHealth.report_failure!()
    assert WriteHealth.healthy?() == false

    WriteHealth.report_recovery!()

    assert WriteHealth.healthy?() == true
  end

  test "report_recovery!/0 is a no-op when already healthy" do
    WriteHealth.report_recovery!()

    assert WriteHealth.healthy?() == true
  end

  test "defaults max_write_health_failures to 3 when unset" do
    Application.delete_env(:logflare, :spool)

    WriteHealth.report_failure!()
    WriteHealth.report_failure!()
    assert WriteHealth.healthy?() == true

    WriteHealth.report_failure!()
    assert WriteHealth.healthy?() == false
  end
end
