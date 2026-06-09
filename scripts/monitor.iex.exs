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

# ---------------------------------------------------------------------------
# Error telemetry counters
# Tracks stale ETS table hits, missing IDs, and stale :processing events.
# ---------------------------------------------------------------------------
stale_table_count  = :atomics.new(1, [])
missing_ids_count  = :atomics.new(1, [])
stale_reset_count  = :atomics.new(1, [])
stale_drop_count   = :atomics.new(1, [])

:telemetry.attach(
  "monitor-stale-table",
  [:logflare, :ingest_event_queue, :stale_table],
  fn _event, %{count: n}, _meta, ref -> :atomics.add(ref, 1, n) end,
  stale_table_count
)

:telemetry.attach(
  "monitor-missing-ids",
  [:logflare, :ingest_event_queue, :missing_ids],
  fn _event, %{count: n}, _meta, ref -> :atomics.add(ref, 1, n) end,
  missing_ids_count
)

:telemetry.attach(
  "monitor-stale-processing",
  [:logflare, :ingest_event_queue, :stale_processing],
  fn _event, %{reset: r, dropped: d}, _meta, {reset_ref, drop_ref} ->
    :atomics.add(reset_ref, 1, r)
    :atomics.add(drop_ref, 1, d)
  end,
  {stale_reset_count, stale_drop_count}
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

    stale_table = :atomics.get(stale_table_count, 1)
    missing_ids = :atomics.get(missing_ids_count, 1)
    stale_reset = :atomics.get(stale_reset_count, 1)
    stale_drop  = :atomics.get(stale_drop_count, 1)

    errors_str =
      if stale_table + missing_ids + stale_reset + stale_drop > 0 do
        " | stale_table=#{stale_table} missing_ids=#{missing_ids} stale_reset=#{stale_reset} stale_drop=#{stale_drop}"
      else
        ""
      end

    IO.puts(
      "ets=#{Float.round(ets / 1_048_576, 1)}MB " <>
        "proc=#{Float.round(proc / 1_048_576, 1)}MB " <>
        "total=#{Float.round(total / 1_048_576, 1)}MB " <>
        "proc_delta=#{Float.round(proc_delta / 1_048_576, 1)}MB | " <>
        "queue_size=#{ingested_size} pending=#{pending}" <>
        errors_str
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
