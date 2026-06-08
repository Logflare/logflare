defmodule Logflare.Bench.PipelineBenchTest do
  @moduledoc """
  Throughput benchmark for the BigQuery pipeline.

  Starts the real Pipeline (with stream_batch commented out) against a real
  source + backend, inserts N events into IngestEventQueue, and reports
  events/sec once all have passed through handle_batch.

  Run with:
    BENCH_EVENTS=100000 mix test test/bench/pipeline_bench_test.exs --timeout 300000
  """
  use Logflare.DataCase, async: false

  @moduletag :bench
  @moduletag timeout: 300_000

  alias Logflare.Backends
  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Sources.Source.BigQuery.Pipeline

  test "pipeline throughput" do
    target = String.to_integer(System.get_env("BENCH_EVENTS", "100000"))

    insert(:plan)
    user    = insert(:user)
    source  = insert(:source, user: user, lock_schema: true)
    backend = insert(:backend, type: :bigquery)

    IngestEventQueue.upsert_tid({source.id, backend.id, nil})

    pipeline_name = Backends.via_source(source, Pipeline, backend.id)

    start_supervised!(
      {DynamicPipeline,
       name: pipeline_name,
       pipeline: Pipeline,
       pipeline_args: [source: source, backend: backend],
       initial_count: 1,
       min_pipelines: 1}
    )

    # Wait for the BufferProducer to register its live queue.
    IO.write("\nWaiting for pipeline queue...")

    Enum.reduce_while(Stream.repeatedly(fn -> Process.sleep(100) end), nil, fn _, _ ->
      live =
        IngestEventQueue.list_queues({source.id, backend.id})
        |> Enum.reject(fn {_, _, pid} -> is_nil(pid) end)

      if live != [], do: {:halt, :ok}, else: {:cont, nil}
    end)

    IO.puts(" ready.")

    # Build events up front so build time isn't counted.
    IO.puts("Building #{target} events...")

    now_us = System.os_time(:microsecond)

    events =
      Enum.map(1..target, fn i ->
        build(:log_event,
          source: source,
          message: "bench #{i}",
          timestamp: DateTime.from_unix!(now_us + i, :microsecond) |> to_string()
        )
      end)

    counter = :atomics.new(1, [])

    :telemetry.attach(
      "bench-pipeline-ack",
      [:logflare, :backends, :pipeline, :ack],
      fn _event, %{successful: n}, _meta, ref -> :atomics.add(ref, 1, n) end,
      counter
    )

    IO.puts("Inserting #{target} events...")

    start_ms = System.monotonic_time(:millisecond)
    IngestEventQueue.add_to_table({source.id, backend.id}, events, check_queue_size: false)
    IO.puts("Inserted. Draining...\n")

    result =
      Enum.reduce_while(Stream.repeatedly(fn -> Process.sleep(100) end), nil, fn _, _ ->
        total = :atomics.get(counter, 1)
        IO.write("\r  processed=#{total}/#{target} (#{round(total / target * 100)}%)")

        if total >= target do
          elapsed_s = (System.monotonic_time(:millisecond) - start_ms) / 1_000
          {:halt, {total, elapsed_s}}
        else
          {:cont, nil}
        end
      end)

    :telemetry.detach("bench-pipeline-ack")

    {total, elapsed_s} = result
    avg_rate = round(total / elapsed_s)

    IO.puts("\n")
    IO.puts("=== Result ===")
    IO.puts("  Events   : #{total}")
    IO.puts("  Elapsed  : #{Float.round(elapsed_s, 2)}s")
    IO.puts("  Rate     : #{avg_rate} events/s")
    IO.puts("")

    assert total >= target
  end
end
