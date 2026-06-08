import_if_available(Logflare.Utils.Debugging)

source_name = System.get_env("MONITOR_SOURCE", "loadfest.test.0")
baseline_proc = :erlang.memory(:processes)

IO.puts("Starting memory monitor for source: #{source_name}")
IO.puts("Baseline proc=#{Float.round(baseline_proc / 1_048_576, 1)}MB\n")

# ---------------------------------------------------------------------------
# Event throughput counter
# Attaches to [:logflare, :backends, :pipeline, :handle_batch] which is
# emitted by both the standard BigQuery pipeline and KafkaProducerPipeline.
# ---------------------------------------------------------------------------
event_counter = :atomics.new(1, [])
test_start_ms = System.monotonic_time(:millisecond)

:telemetry.attach(
  "monitor-pipeline-handle-batch",
  [:logflare, :backends, :pipeline, :handle_batch],
  fn _event, %{batch_size: size}, _meta, ref ->
    :atomics.add(ref, 1, size)
  end,
  event_counter
)

# Memory + queue monitor
spawn(fn ->
  Stream.repeatedly(fn ->
    ets = :erlang.memory(:ets)
    proc = :erlang.memory(:processes)
    total = :erlang.memory(:total)
    proc_delta = proc - baseline_proc

    source = Logflare.Sources.get_by(name: source_name)

    {pending, ingested_size} =
      if source do
        pending = Logflare.Backends.IngestEventQueue.total_pending({source.id, nil})
        ingested_size = Logflare.Backends.IngestEventQueue.get_table_size({source.id, nil, nil})
        {pending, ingested_size}
      else
        {:"source_not_found", :"source_not_found"}
      end

    IO.puts(
      "ets=#{Float.round(ets / 1_048_576, 1)}MB " <>
        "proc=#{Float.round(proc / 1_048_576, 1)}MB " <>
        "total=#{Float.round(total / 1_048_576, 1)}MB " <>
        "proc_delta=#{Float.round(proc_delta / 1_048_576, 1)}MB | " <>
        "queue_size=#{ingested_size} pending=#{pending}"
    )

    Process.sleep(1_000)
  end)
  |> Stream.run()
end)

# Throughput monitor — prints on its own line every second
spawn(fn ->
  loop = fn loop, prev_total ->
    Process.sleep(1_000)
    elapsed_s = (System.monotonic_time(:millisecond) - test_start_ms) / 1_000
    total = :atomics.get(event_counter, 1)
    rate = total - prev_total

    IO.puts(
      "  → events=#{total} " <>
        "elapsed=#{Float.round(elapsed_s, 1)}s " <>
        "rate=#{rate}/s"
    )

    loop.(loop, total)
  end

  loop.(loop, 0)
end)
