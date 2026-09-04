defmodule Logflare.Backends.Spool.WriteHealthTest do
  # Global, process-independent state (mirrors Logflare.Readiness) — async:
  # false so this never overlaps with any other test reading/writing it.
  use ExUnit.Case, async: false

  alias Logflare.Backends.Spool.WriteHealth

  setup do
    on_exit(fn -> WriteHealth.report_recovery!() end)
    :ok
  end

  test "healthy?/0 is true by default" do
    assert WriteHealth.healthy?() == true
  end

  test "report_failure!/0 marks it unhealthy" do
    WriteHealth.report_failure!()

    assert WriteHealth.healthy?() == false
  end

  test "report_recovery!/0 clears an unhealthy state" do
    WriteHealth.report_failure!()
    assert WriteHealth.healthy?() == false

    WriteHealth.report_recovery!()

    assert WriteHealth.healthy?() == true
  end

  test "report_recovery!/0 is a no-op when already healthy" do
    WriteHealth.report_recovery!()

    assert WriteHealth.healthy?() == true
  end
end
