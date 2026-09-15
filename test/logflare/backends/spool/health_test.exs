defmodule Logflare.Backends.Spool.HealthTest do
  # Global, process-independent state (mirrors Logflare.Readiness) — async:
  # false so this never overlaps with any other test reading/writing it.
  use ExUnit.Case, async: false

  alias Logflare.Backends.Spool.Health

  setup do
    prev_spool_config = Application.get_env(:logflare, :spool)

    on_exit(fn ->
      Health.report_recovery!(:disk)
      Health.report_recovery!(:upload)

      if prev_spool_config do
        Application.put_env(:logflare, :spool, prev_spool_config)
      else
        Application.delete_env(:logflare, :spool)
      end
    end)

    :ok
  end

  test "healthy?/1 is true by default for both scopes" do
    assert Health.healthy?(:disk) == true
    assert Health.healthy?(:upload) == true
  end

  test "report_failure!/1 marks only the reported scope unhealthy once max_spool_health_failures is reached" do
    Application.put_env(:logflare, :spool, max_spool_health_failures: 3)

    Health.report_failure!(:disk)
    assert Health.healthy?(:disk) == true

    Health.report_failure!(:disk)
    assert Health.healthy?(:disk) == true

    Health.report_failure!(:disk)
    assert Health.healthy?(:disk) == false
    assert Health.healthy?(:upload) == true
  end

  test "report_recovery!/1 resets the failure count, not just the healthy flag" do
    Application.put_env(:logflare, :spool, max_spool_health_failures: 3)

    Health.report_failure!(:upload)
    Health.report_failure!(:upload)
    Health.report_recovery!(:upload)

    # If the count hadn't reset, one more failure would already reach 3 and
    # flip it unhealthy — proving it takes another full run of 3 shows the
    # counter itself was cleared, not just the healthy flag.
    Health.report_failure!(:upload)
    assert Health.healthy?(:upload) == true

    Health.report_failure!(:upload)
    assert Health.healthy?(:upload) == true

    Health.report_failure!(:upload)
    assert Health.healthy?(:upload) == false
  end

  test "report_recovery!/1 clears an unhealthy state" do
    Application.put_env(:logflare, :spool, max_spool_health_failures: 1)
    Health.report_failure!(:disk)
    assert Health.healthy?(:disk) == false

    Health.report_recovery!(:disk)

    assert Health.healthy?(:disk) == true
  end

  test "report_recovery!/1 is a no-op when already healthy" do
    Health.report_recovery!(:disk)

    assert Health.healthy?(:disk) == true
  end

  test "defaults max_spool_health_failures to 3 when unset" do
    Application.delete_env(:logflare, :spool)

    Health.report_failure!(:disk)
    Health.report_failure!(:disk)
    assert Health.healthy?(:disk) == true

    Health.report_failure!(:disk)
    assert Health.healthy?(:disk) == false
  end

  test "a failing scope doesn't affect the other scope" do
    Application.put_env(:logflare, :spool, max_spool_health_failures: 1)

    Health.report_failure!(:disk)

    assert Health.healthy?(:disk) == false
    assert Health.healthy?(:upload) == true
  end

  test "emits telemetry reflecting the current healthy/failure_count state on every report, tagged by scope" do
    Application.put_env(:logflare, :spool, max_spool_health_failures: 2)
    test_pid = self()
    ref = make_ref()

    :telemetry.attach(
      {__MODULE__, ref},
      [:logflare, :backends, :spool, :write_health],
      fn _event, measurements, metadata, _ -> send(test_pid, {ref, measurements, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)

    Health.report_failure!(:upload)
    assert_receive {^ref, %{healthy: 1, failure_count: 1}, %{scope: :upload}}

    Health.report_failure!(:upload)
    assert_receive {^ref, %{healthy: 0, failure_count: 2}, %{scope: :upload}}

    Health.report_recovery!(:upload)
    assert_receive {^ref, %{healthy: 1, failure_count: 0}, %{scope: :upload}}
  end
end
