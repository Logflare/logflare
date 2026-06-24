# Pipeline throughput benchmark.
#
# Inserts N events directly into IngestEventQueue and measures time until
# all events have passed through handle_batch. Bypasses HTTP so there is
# no network jitter — results are directly comparable across branches.
#
# Compatible with: test/mem_usage, test/mem_main_controll
#
# Usage — paste into a running iex -S mix session:
#   source_name = "loadfest.test.0"; target = 100_000
#   import_if_available(Logflare.Utils.Debugging)
#
# Or load directly:
#   IEx.Helpers.c "scripts/bench.iex.exs"

alias Logflare.Backends.IngestEventQueue
alias Logflare.LogEvent
alias Logflare.Sources

source_name = System.get_env("BENCH_SOURCE", "loadfest.test.0")
target = String.to_integer(System.get_env("BENCH_EVENTS", "100000"))

source = Sources.get_by(name: source_name)

unless source do
  IO.puts("ERROR: source #{inspect(source_name)} not found")
  System.halt(1)
end

# Wait for at least one live pipeline queue to be registered.
# Queues with pid=nil are startup/drain queues that no pipeline reads from.
IO.write("Waiting for pipeline queues to be ready...")

:ok =
  Stream.repeatedly(fn -> Process.sleep(200) end)
  |> Stream.each(fn _ -> IO.write(".") end)
  |> Enum.reduce_while(:waiting, fn _, _ ->
    live_queues =
      IngestEventQueue.list_queues({source.id, nil})
      |> Enum.reject(fn {_, _, pid} -> is_nil(pid) end)

    if live_queues != [], do: {:halt, :ok}, else: {:cont, :waiting}
  end)

IO.puts(" ready.\n")

IO.puts("Building #{target} events...")

now_us = System.os_time(:microsecond)

events =
  Enum.map(1..target, fn i ->
    LogEvent.make(
      %{"message" => "bench event #{i}", "timestamp" => now_us + i},
      %{source: source}
    )
  end)

counter = :atomics.new(1, [])

# Detach any leftover handler from a previous crashed run before attaching a fresh one.
:telemetry.detach("bench-handle-batch")

:ok =
  :telemetry.attach(
    "bench-handle-batch",
    [:logflare, :backends, :pipeline, :handle_batch],
    fn _event, %{batch_size: size}, _meta, ref -> :atomics.add(ref, 1, size) end,
    counter
  )

IO.puts("Inserting #{target} events...")

start_ms = System.monotonic_time(:millisecond)
timeout_ms = 120_000
IngestEventQueue.add_to_table({source.id, nil}, events, check_queue_size: false)

IO.puts("Inserted. Waiting for pipeline to drain...\n")

Stream.repeatedly(fn ->
  total = :atomics.get(counter, 1)
  elapsed_ms = System.monotonic_time(:millisecond) - start_ms

  cond do
    total >= target ->
      elapsed_s = elapsed_ms / 1_000
      avg_rate = round(target / elapsed_s)

      IO.puts(
        "\nDONE: #{target} events in #{Float.round(elapsed_s, 2)}s = #{avg_rate}/s avg"
      )

      :telemetry.detach("bench-handle-batch")
      System.halt(0)

    elapsed_ms >= timeout_ms ->
      IO.puts("\nTIMEOUT: only #{total}/#{target} events processed in #{timeout_ms / 1_000}s")
      :telemetry.detach("bench-handle-batch")
      System.halt(1)

    true ->
      IO.write("\r  processed=#{total}/#{target} (#{round(total / max(target, 1) * 100)}%)")
      Process.sleep(100)
  end
end)
|> Stream.run()
