defmodule Logflare.TelemetryTest do
  use Logflare.DataCase
  alias Logflare.Telemetry

  describe "process metrics" do
    test "retrieves and emits top 10 by memory" do
      event = [:logflare, :system, :top_processes, :memory]
      attach_forwarder(event)

      Telemetry.process_memory_metrics()

      1..10
      |> Enum.each(fn _ ->
        assert_receive {:telemetry_event, ^event, metrics, meta}

        assert match?(%{size: _}, metrics)
        assert match?(%{name: _}, meta)
      end)

      refute_receive {:telemetry_event, ^event, _, _}
    end

    test "retrieves and emits top 10 by message queue" do
      event = [:logflare, :system, :top_processes, :message_queue]
      attach_forwarder(event)

      Telemetry.process_message_queue_metrics()

      1..10
      |> Enum.each(fn _ ->
        assert_receive {:telemetry_event, ^event, metrics, meta}

        assert match?(%{length: _}, metrics)
        assert match?(%{name: _}, meta)
      end)

      refute_receive {:telemetry_event, ^event, _, _}
    end
  end

  describe "ets_table_metrics/1" do
    test "retrieves and emits top 10 by table size" do
      event = [:logflare, :system, :top_ets_tables, :individual]
      attach_forwarder(event)

      Telemetry.ets_table_metrics()

      1..10
      |> Enum.each(fn _ ->
        assert_receive {:telemetry_event, ^event, metrics, meta}

        assert match?(%{size: _}, metrics)
        assert match?(%{name: _}, meta)
      end)

      refute_receive {:telemetry_event, ^event, _, _}
    end

    test "retrieves and emits top 100 by table size" do
      event = [:logflare, :system, :top_ets_tables, :grouped]
      attach_forwarder(event)

      Telemetry.ets_table_metrics()

      1..100
      |> Enum.each(fn _ ->
        assert_receive {:telemetry_event, ^event, metrics, meta}

        assert match?(%{size: _}, metrics)
        assert match?(%{name: _}, meta)
      end)

      refute_receive {:telemetry_event, ^event, _, _}
    end
  end

  defp attach_forwarder(event_name, opts \\ []) do
    test_pid = Keyword.get(opts, :pid, self())
    id = "test-telemetry-" <> Base.encode16(:erlang.term_to_binary(make_ref()))

    :ok =
      :telemetry.attach(
        id,
        event_name,
        fn ^event_name, measurements, metadata, _config ->
          send(test_pid, {:telemetry_event, event_name, measurements, metadata})
        end,
        nil
      )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(id) end)
    id
  end
end
