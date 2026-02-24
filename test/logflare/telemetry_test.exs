defmodule Logflare.TelemetryTest do
  use Logflare.DataCase, async: true

  alias Logflare.Telemetry
  alias Logflare.TestUtils

  describe "process metrics" do
    test "retrieves and emits top 10 by memory" do
      event = [:logflare, :system, :top_processes, :memory]
      TestUtils.attach_forwarder(event)
      Telemetry.process_memory_metrics()

      assert_receive {:telemetry_event, ^event, metrics, meta}
      assert match?(%{size: _}, metrics)
      assert match?(%{name: _}, meta)
    end

    test "retrieves and emits top 10 by message queue" do
      event = [:logflare, :system, :top_processes, :message_queue]
      TestUtils.attach_forwarder(event)
      Telemetry.process_message_queue_metrics()

      assert_receive {:telemetry_event, ^event, metrics, meta}
      assert match?(%{length: _}, metrics)
      assert match?(%{name: _}, meta)
    end
  end

  describe "ets_table_metrics/1" do
    test "retrieves and emits top 10 by memory usage" do
      event = [:logflare, :system, :top_ets_tables, :individual]
      TestUtils.attach_forwarder(event)
      Telemetry.ets_table_metrics()

      assert_receive {:telemetry_event, ^event, metrics, meta}
      assert match?(%{memory: _}, metrics)
      assert match?(%{name: _}, meta)
    end

    test "retrieves and emits top 100 by memory usage" do
      event = [:logflare, :system, :top_ets_tables, :grouped]
      TestUtils.attach_forwarder(event)
      Telemetry.ets_table_metrics()

      assert_receive {:telemetry_event, ^event, metrics, meta}
      assert match?(%{memory: _}, metrics)
      assert match?(%{name: _}, meta)
    end

    test "ignores tables that were deleted during listing" do
      # simulates all tables being deleted to simplify testing, so we can
      # just check if all tables were skipped for returning :undefined
      Logflare.Utils
      |> stub(:ets_info, fn _ -> :undefined end)

      event = [:logflare, :system, :top_ets_tables, :individual]
      TestUtils.attach_forwarder(event)
      Telemetry.ets_table_metrics()

      refute_receive {:telemetry_event, ^event, _, _}
    end
  end

  test "observer metrics" do
    ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :system, :observer, :metrics],
        [:logflare, :system, :observer, :memory]
      ])

    on_exit(fn -> :telemetry.detach(ref) end)
    Logflare.SystemMetrics.Observer.dispatch_stats()

    assert_receive {[:logflare, :system, :observer, :metrics], ^ref, metrics_measurements,
                    metrics_metadata}

    assert_receive {[:logflare, :system, :observer, :memory], ^ref, memory_measurements,
                    memory_metadata}

    assert metrics_measurements |> Map.keys() |> Enum.sort() == [
             :atom_count,
             :atom_limit,
             :ets_count,
             :ets_limit,
             :io_input,
             :io_output,
             :logical_processors,
             :logical_processors_available,
             :logical_processors_online,
             :otp_release,
             :port_count,
             :port_limit,
             :process_count,
             :process_limit,
             :run_queue,
             :schedulers,
             :schedulers_online,
             :total_active_tasks,
             :uptime
           ]

    # All measurement values must be numeric (integers or floats)
    for {key, value} <- metrics_measurements do
      assert is_number(value), "expected #{key} to be numeric, got: #{inspect(value)}"
    end

    assert metrics_metadata == %{}

    assert memory_measurements |> Map.keys() |> Enum.sort() == [
             :atom,
             :atom_used,
             :binary,
             :code,
             :ets,
             :persistent_term,
             :processes,
             :processes_used,
             :system,
             :total
           ]

    for {key, value} <- memory_measurements do
      assert is_number(value), "expected #{key} to be numeric, got: #{inspect(value)}"
    end

    assert memory_metadata == %{}
  end

  test "scheduler metrics" do
    event = [:logflare, :system, :scheduler, :utilization]
    ref = :telemetry_test.attach_event_handlers(self(), [event])
    on_exit(fn -> :telemetry.detach(ref) end)

    sample_duration = to_timeout(millisecond: 10)
    Logflare.SystemMetrics.Schedulers.async_dispatch_stats(sample_duration)

    assert_receive {^event, ^ref, %{utilization: _}, %{name: "total", type: "total"}}
    assert_receive {^event, ^ref, %{utilization: _}, %{name: "weighted", type: "weighted"}}

    for id <- 1..:erlang.system_info(:schedulers) do
      id = Integer.to_string(id)
      assert_receive {^event, ^ref, %{utilization: _}, %{name: ^id, type: "normal"}}
    end

    dirty_cpu_offset = :erlang.system_info(:schedulers)

    for id <-
          (dirty_cpu_offset + 1)..(dirty_cpu_offset + :erlang.system_info(:dirty_cpu_schedulers)) do
      id = Integer.to_string(id)
      assert_receive {^event, ^ref, %{utilization: _}, %{name: ^id, type: "dirty"}}
    end

    dirty_io_offset = dirty_cpu_offset + :erlang.system_info(:dirty_cpu_schedulers)

    for id <- (dirty_io_offset + 1)..(dirty_io_offset + :erlang.system_info(:dirty_io_schedulers)) do
      id = Integer.to_string(id)
      assert_receive {^event, ^ref, %{utilization: _}, %{name: ^id, type: "dirty (io)"}}
    end
  end

  describe "cachex_metrics/0" do
    test "retrieves and emits stats for caches with all metrics" do
      expected_metrics = [
        :log_events,
        :rejected_log_events,
        :pub_sub_rates,
        :team_users,
        :partners,
        :users,
        :backends,
        :sources,
        :billing,
        :source_schemas,
        :auth,
        :endpoints,
        :rules,
        :key_values,
        :saved_searches
      ]

      events = Enum.map(expected_metrics, fn metric -> [:cachex, metric] end)
      ref = :telemetry_test.attach_event_handlers(self(), events)
      on_exit(fn -> :telemetry.detach(ref) end)

      # simulates telemetry_poller tick
      Telemetry.cachex_metrics()

      for event <- events do
        assert_receive {^event, ^ref, measurements, metadata}

        assert measurements |> Map.keys() |> Enum.sort() == [
                 :evictions,
                 :expirations,
                 :hit_rate,
                 :hits,
                 :miss_rate,
                 :misses,
                 :operations,
                 :purge,
                 :stats,
                 :total_heap_size
               ]

        for {key, value} <- measurements do
          assert is_number(value),
                 "expected #{key} for #{inspect(event)} to be numeric, got: #{inspect(value)}"
        end

        assert metadata == %{}
      end

      refute_received _anything_else
    end
  end
end
