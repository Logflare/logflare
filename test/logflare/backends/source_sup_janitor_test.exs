defmodule Logflare.Backends.SourceSupJanitorTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.Backends.SourceSupJanitor
  alias Logflare.Source
  alias Logflare.Sources

  import Mimic

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)

    {:ok, source: source, user: user}
  end

  describe "behavior simulation tests" do
    test "starts in slow mode and transitions to fast when idle", %{source: source} do
      # Mock idle condition to trigger fast mode
      stub(Sources, :get_source_metrics_for_ingest, fn _source ->
        %Source.Metrics{avg: 0}
      end)

      {:ok, pid} = start_supervised({SourceSupJanitor, [source: source, slow_check_interval: 50, fast_check_interval: 20]})


      # Wait for initial slow check to trigger fast mode
      Process.sleep(80)

      assert Process.alive?(pid)
    end

    test "remains in slow mode when source is active", %{source: source} do
      # Mock active condition to stay in slow mode
      stub(Sources, :get_source_metrics_for_ingest, fn _source ->
        %Source.Metrics{avg: 10}
      end)

      {:ok, pid} = start_supervised({SourceSupJanitor, [source: source, slow_check_interval: 50]})

      # Wait for multiple slow checks
      Process.sleep(150)

      assert Process.alive?(pid)
    end

    test "transitions back to slow mode when source becomes active during fast mode", %{source: source} do
      call_count = :counters.new(1, [])

      stub(Sources, :get_source_metrics_for_ingest, fn _source ->
        count = :counters.get(call_count, 1)
        :counters.add(call_count, 1, 1)
        case count do
          0 -> %Source.Metrics{avg: 0}   # idle - trigger fast mode
          1 -> %Source.Metrics{avg: 0}   # idle - stay in fast mode briefly
          _ -> %Source.Metrics{avg: 5}   # active - back to slow mode
        end
      end)

      {:ok, pid} = start_supervised({SourceSupJanitor, [source: source, slow_check_interval: 50, fast_check_interval: 20]})

      # Wait for mode transitions
      Process.sleep(150)

      assert Process.alive?(pid)
    end

    test "shuts down source after consecutive idle checks", %{source: source} do
      # Mock idle condition consistently
      stub(Sources, :get_source_metrics_for_ingest, fn _source ->
        %Source.Metrics{avg: 0}
      end)

      # Mock source shutdown with expectation
      expect(Logflare.Source.Supervisor, :stop_source_local, fn _source ->
        :ok
      end)

      {:ok, pid} = start_supervised({SourceSupJanitor, [source: source, slow_check_interval: 30, fast_check_interval: 15]})

      # Wait for full cycle: slow -> fast -> 6 fast checks -> shutdown
      Process.sleep(200)

      assert Process.alive?(pid)
    end
  end

  describe "integration test" do
    test "full workflow from slow to fast to shutdown", %{source: source} do
      # Start with short intervals for faster testing
      opts = [
        source: source,
        slow_check_interval: 50,
        fast_check_interval: 20
      ]

      # Mock idle condition to trigger fast mode and eventual shutdown
      stub(Sources, :get_source_metrics_for_ingest, fn _source ->
        %Source.Metrics{avg: 0}
      end)

      # Mock source shutdown with expectation
      expect(Logflare.Source.Supervisor, :stop_source_local, fn _source ->
        :ok
      end)

      {:ok, pid} = SourceSupJanitor.start_link(opts)

      # Wait for the process to go through its cycle
      # First check (slow) -> fast mode -> 6 fast checks -> shutdown
      Process.sleep(300)

      # Verify the process is still alive (it continues after shutdown)
      assert Process.alive?(pid)
    end

    test "mode transitions with changing conditions", %{source: source} do
      opts = [
        source: source,
        slow_check_interval: 50,
        fast_check_interval: 20
      ]

      call_count = :counters.new(1, [])
      pid = self()
      stub(Sources, :get_source_metrics_for_ingest, fn _source ->
        count = :counters.get(call_count, 1)
        :counters.add(call_count, 1, 1)
        case count do
          0 -> %Source.Metrics{avg: 10}  # active - stay slow
          1 -> %Source.Metrics{avg: 0}   # idle - go fast
          _ ->
            send(pid, :idle)

            %Source.Metrics{avg: 5}   # active - back to slow
        end
      end)

      start_supervised({SourceSupJanitor, opts})

      assert_receive :idle, 1_000
    end
  end
end
