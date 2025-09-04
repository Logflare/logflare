defmodule Logflare.TelemetryTest do
  use ExUnit.Case, async: false

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
    test "retrieves and emits top 10 by table size" do
      event = [:logflare, :system, :top_ets_tables, :individual]
      TestUtils.attach_forwarder(event)
      Telemetry.ets_table_metrics()

      assert_receive {:telemetry_event, ^event, metrics, meta}
      assert match?(%{size: _}, metrics)
      assert match?(%{name: _}, meta)
    end

    test "retrieves and emits top 100 by table size" do
      event = [:logflare, :system, :top_ets_tables, :grouped]
      TestUtils.attach_forwarder(event)
      Telemetry.ets_table_metrics()

      assert_receive {:telemetry_event, ^event, metrics, meta}
      assert match?(%{size: _}, metrics)
      assert match?(%{name: _}, meta)
    end
  end
end
