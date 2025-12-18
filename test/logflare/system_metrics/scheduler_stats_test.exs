defmodule Logflare.SystemMetrics.SchedulerStatsTest do
  use Logflare.DataCase, async: false
  alias Logflare.SystemMetrics.Observer
  alias Logflare.SystemMetrics.Schedulers.Poller

  test "observer dispatch_stats emits telemetry events", %{test: test} do
    handler_id = "#{test}-observer-handler"
    test_pid = self()

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    :telemetry.attach_many(
      handler_id,
      [
        [:logflare, :system, :observer, :metrics],
        [:logflare, :system, :observer, :memory]
      ],
      fn event_name, measurements, _metadata, _config ->
        send(test_pid, {:telemetry_event, event_name, measurements})
      end,
      nil
    )

    Observer.dispatch_stats()

    assert_receive {:telemetry_event, [:logflare, :system, :observer, :metrics], _measurements}
    assert_receive {:telemetry_event, [:logflare, :system, :observer, :memory], _measurements}
  end

  test "scheduler poller emits telemetry events", %{test: test} do
    handler_id = "#{test}-scheduler-handler"
    test_pid = self()

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    :telemetry.attach(
      handler_id,
      [:logflare, :system, :scheduler, :utilization],
      fn event_name, measurements, metadata, _config ->
        send(test_pid, {:telemetry_event, event_name, measurements, metadata})
      end,
      nil
    )

    # Simulate Poller logic (we can't easily force the GenServer to tick without waiting,
    # so we'll test the core logic if possible, or just send the message to the GenServer if it's named/registered)
    # The poller starts automatically in the supervision tree, so we can potentially just wait for it,
    # OR we can manually invoke the logic if we extract it.
    # Given the Poller is simple, triggering `handle_info` is hard without access to private state (last metrics).
    # Instead, we'll verify by starting a fresh, isolated Poller process for the test?
    # Actually, the Poller logic relies on `:scheduler.sample()`.

    # Let's instantiate a temporary poller to test this isolation
    {:ok, _pid} = GenServer.start_link(Poller, [], [])

    # Send polling message
    # The poller waits 5s by default. We can force it by sending :poll_metrics?
    # But `handle_info` expects `last_scheduler_metrics` as state.
    # The best way is to manually call the core logic if it was exposed, but it's not.

    # We'll just rely on the fact that `handle_info(:poll_metrics, state)` does the work.
    # We can assume the existing Poller in the system might fire, but better to control it.

    # Let's try sending the message to the Poller and see if it responds?
    # `handle_info` returns `{:noreply, current_scheduler_metrics}`.

    # Actually, let's just assert on the side effect of `Poller` which is the telemetry event.
    # We can manually trigger the loop by sending a message if we knew the Pid.
    # But since we modified `handle_info` to doing the work, let's check the code:
    # def handle_info(:poll_metrics, last_scheduler_metrics)

    # We can start a `Poller` under test control.
    {:ok, _pid} = GenServer.start_link(Poller, [], name: :test_poller)

    # Wait for init? Init calls `poll_metrics(random)`.
    # We can send `:poll_metrics` manually to our named process.
    # But `handle_info` needs the state...

    # Simple approach: Verification on the running system is safer here, but for unit test,
    # we can trust that `start_link` works.
    # Let's just wait for an event? No, random delay is up to 60s.

    # Let's try to unit test by invoking `handle_info` directly?
    # It's not `defp`, so we CAN call it if we pass a valid state.
    state = :scheduler.sample()
    Poller.handle_info(:poll_metrics, state)

    assert_receive {:telemetry_event, [:logflare, :system, :scheduler, :utilization],
                    measurements, metadata}

    assert Map.has_key?(measurements, :utilization)
    assert Map.has_key?(metadata, :name)
    assert Map.has_key?(metadata, :type)
  end
end
