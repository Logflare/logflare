# Benchmarks the CPU-sensitive ClickHouse pipeline change from PR #3667.
#
# It compares the old batch path against the new incremental gzip path using the
# same generated events and the same RowBinary encoder:
#
#   old:    map all events -> Ingester.encode_batch/2 -> :zlib.gzip/1
#   stream: map one event  -> Ingester.encode_row/2   -> :zlib.deflate/3
#   ets:    stream path plus the extra ETS lookup performed by id-passing batches
#
# Usage examples:
#
#   mix run --no-start bench/clickhouse_pipeline_encode_compress_cpu.exs
#   EVENT_TYPES=log BATCH_SIZES=1000,10000 SHAPES=small,realistic \
#     mix run --no-start bench/clickhouse_pipeline_encode_compress_cpu.exs
#
# Use Benchee's reduction/memory metrics to understand BEAM-side overhead, but
# use bench/clickhouse_pipeline_cpu_runner.exs under /usr/bin/time for the final
# "does this consume more CPU seconds per row?" answer, because zlib work happens
# in native code and is not fully represented by reductions.

Code.require_file("support/clickhouse_pipeline_bench_data.exs", __DIR__)

alias Logflare.Bench.ClickHousePipelineData, as: Data

parse_csv = fn env, default, mapper ->
  env
  |> System.get_env(default)
  |> String.split(",", trim: true)
  |> Enum.map(mapper)
end

batch_sizes = parse_csv.("BATCH_SIZES", "1000,10000", &String.to_integer/1)
event_types = parse_csv.("EVENT_TYPES", "log,metric,trace", &String.to_existing_atom/1)
shapes = parse_csv.("SHAPES", "realistic", &String.to_existing_atom/1)
benchee_time = System.get_env("BENCH_TIME", "5") |> String.to_integer()
benchee_warmup = System.get_env("BENCH_WARMUP", "2") |> String.to_integer()

inputs =
  for shape <- shapes,
      type <- event_types,
      batch_size <- batch_sizes,
      into: %{} do
    {compiled, config_id} = Data.compiled(type)
    events = Data.batch(type, batch_size, shape)
    processing_tid = Data.setup_processing_ets(events)
    encoded_bytes = Data.encoded_bytes(events, type, compiled, config_id)

    label = "#{type}/#{shape}/batch=#{batch_size}"

    IO.puts(
      "#{label}: #{encoded_bytes} uncompressed RowBinary bytes " <>
        "(#{Float.round(encoded_bytes / batch_size, 1)} bytes/event)"
    )

    {label,
     %{
       events: events,
       type: type,
       compiled: compiled,
       config_id: config_id,
       processing_tid: processing_tid
     }}
  end

IO.puts("")

Benchee.run(
  %{
    "old encode_batch + gzip" => fn input ->
      Data.run_scenario(:old, input)
    end,
    "old materialize + gzip" => fn input ->
      Data.run_scenario(:old_materialized, input)
    end,
    "new stream deflate" => fn input ->
      Data.run_scenario(:stream, input)
    end,
    "new stream deflate (hoisted uuid)" => fn input ->
      Data.run_scenario(:stream_hoisted, input)
    end,
    "new stream deflate (chunked)" => fn input ->
      Data.run_scenario(:stream_chunked, input)
    end,
    "new stream deflate + ETS lookup" => fn input ->
      Data.run_scenario(:stream_ets, input)
    end,
    "new stream deflate + ETS lookup (hoisted uuid)" => fn input ->
      Data.run_scenario(:stream_ets_hoisted, input)
    end,
    "new stream deflate + ETS lookup (chunked)" => fn input ->
      Data.run_scenario(:stream_ets_chunked, input)
    end
  },
  inputs: inputs,
  time: benchee_time,
  warmup: benchee_warmup,
  memory_time: System.get_env("BENCH_MEMORY_TIME", "2") |> String.to_integer(),
  reduction_time: System.get_env("BENCH_REDUCTION_TIME", "2") |> String.to_integer(),
  print: [configuration: false]
)
