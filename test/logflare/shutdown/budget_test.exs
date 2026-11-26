defmodule Logflare.Shutdown.BudgetTest do
  use ExUnit.Case, async: true

  alias Logflare.Shutdown.Budget

  @now -100_000

  defp plan(overrides) do
    Budget.plan(
      @now,
      Keyword.merge(
        [
          deadline_ms: @now + 80_000,
          withdrawal_ms: 15_000,
          request_timeout_ms: 30_000,
          drain_timeout_ms: 60_000,
          spool_buffer: :spool
        ],
        overrides
      )
    )
  end

  test "reserves separate sync and cleanup windows after the latest queue cutoff" do
    budget = plan([])
    assert budget.withdrawal == @now + 15_000
    assert budget.listeners == {@now + 57_000, 1_000}
    assert budget.requests == {@now + 58_000, 30_000}
    assert budget.writes == {@now + 59_000, 1_000}
    assert budget.flush.queues == {@now + 60_000, 60_000}
    assert budget.flush.sync == {@now + 70_000, 10_000}
    assert budget.flush.cleanup == {@now + 80_000, 10_000}
  end

  test "does not reserve sync time when there is no spool buffer" do
    budget = plan(spool_buffer: nil)
    assert budget.flush.queues == {@now + 70_000, 60_000}
    assert budget.flush.sync == {@now + 70_000, 0}
    assert budget.flush.cleanup == {@now + 80_000, 10_000}
  end

  test "a longer withdrawal leaves time for each stage with the default overall budget" do
    budget = plan(deadline_ms: @now + 65_000, withdrawal_ms: 45_000, drain_timeout_ms: 30_000)
    {listeners_end, _} = budget.listeners
    {requests_end, _} = budget.requests
    {writes_end, _} = budget.writes
    {queues_end, _} = budget.flush.queues

    assert listeners_end - budget.withdrawal == 1_000
    assert requests_end - listeners_end == 1_000
    assert writes_end - requests_end == 1_000
    assert queues_end - writes_end == 1_000
  end

  test "clips withdrawal and leaves both sync and cleanup time on a short budget" do
    budget = plan(deadline_ms: @now + 200)
    assert budget.withdrawal == @now + 52
    assert budget.listeners == {@now + 64, 1_000}
    assert budget.requests == {@now + 76, 30_000}
    assert budget.writes == {@now + 88, 1_000}
    assert budget.flush.queues == {@now + 100, 60_000}
    assert budget.flush.sync == {@now + 150, 10_000}
    assert budget.flush.cleanup == {@now + 200, 10_000}
  end

  test "limits withdrawal and intake to leave time to close admission without a final flush" do
    budget = plan(deadline_ms: @now + 200, drain_timeout_ms: nil)

    assert budget.withdrawal == @now + 125
    assert budget.intake == {@now + 125, 1_000}
    assert budget.listeners == {@now + 150, 1_000}
  end

  test "does not reserve time for zero-length request and queue waits" do
    budget = plan(deadline_ms: @now + 200, request_timeout_ms: 0, drain_timeout_ms: 0)
    assert budget.listeners == {@now + 88, 1_000}
    assert budget.requests == {@now + 88, 0}
    assert budget.writes == {@now + 100, 1_000}
    assert budget.flush.queues == {@now + 100, 0}
  end

  test "does not reserve time for a disabled final flush" do
    budget = plan(drain_timeout_ms: nil)
    assert budget.requests == {@now + 79_000, 30_000}
    assert budget.writes == {@now + 80_000, 1_000}
    assert budget.flush == nil
  end
end
