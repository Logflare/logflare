defmodule Logflare.SystemMetrics.ObserverTest do
  use Logflare.DataCase, async: false
  alias Logflare.SystemMetrics.Observer

  test "dispatch_stats emits observer metrics telemetry event" do
    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [[:logflare, :system, :observer, :metrics]])

    on_exit(fn -> :telemetry.detach(telemetry_ref) end)

    # imitate telemetry_poller tick
    Observer.dispatch_stats()

    assert_received {[:logflare, :system, :observer, :metrics], ^telemetry_ref, measurements,
                     _metadata}

    assert Map.keys(measurements) == [
             :port_count,
             :port_limit,
             :process_count,
             :process_limit,
             :run_queue,
             :schedulers_online,
             :total_active_tasks,
             :version,
             :otp_release,
             :schedulers,
             :uptime,
             :logical_processors,
             :logical_processors_online,
             :logical_processors_available,
             :atom_limit,
             :atom_count,
             :ets_limit,
             :ets_count,
             :schedulers_available,
             :io_output,
             :io_input
           ]
  end

  test "dispatch_stats emits observer memory telemetry event" do
    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [[:logflare, :system, :observer, :memory]])

    on_exit(fn -> :telemetry.detach(telemetry_ref) end)

    # imitate telemetry_poller tick
    Observer.dispatch_stats()

    assert_received {[:logflare, :system, :observer, :memory], ^telemetry_ref, measurements,
                     _metadata}

    assert Map.keys(measurements) == [
             :atom,
             :atom_used,
             :binary,
             :code,
             :ets,
             :processes,
             :processes_used,
             :system,
             :total
           ]

    Enum.each(measurements, fn {key, value} ->
      assert is_integer(value), "Value #{value} for key #{key} is not an integer"
      assert value >= 0, "Value #{value} for key #{key} is not greater than or equal to 0"
    end)
  end
end
