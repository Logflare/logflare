# Queue-level CPU benchmark for PR #3667's ClickHouse id-passing pipeline.
#
# This includes the ETS queue operations that the isolated encode/compress bench
# intentionally omits:
#
#   old queue path: add_to_table -> pop_pending -> encode_batch -> gzip
#   id queue path:  add_to_table -> pop_pending_pointers -> routing lookup ->
#                   per-row ETS lookup -> stream deflate -> delete_id ack
#
# It still avoids ClickHouse/network I/O so the result is focused on local CPU,
# ETS, mapping, encoding, compression, and ack/delete overhead.
#
# Usage:
#
#   mix run --no-start bench/clickhouse_pipeline_queue_cpu.exs
#   EVENT_TYPES=log BATCH_SIZES=1000,10000 SHAPES=realistic \
#     mix run --no-start bench/clickhouse_pipeline_queue_cpu.exs
#
# For fixed-work CPU seconds per row, use bench/clickhouse_pipeline_cpu_runner.exs
# with SCENARIO=old_queue and SCENARIO=id_queue under /usr/bin/time.

Code.require_file("support/clickhouse_pipeline_bench_data.exs", __DIR__)

alias Logflare.Bench.ClickHousePipelineData, as: Data

parse_csv = fn env, default, mapper ->
  env
  |> System.get_env(default)
  |> String.split(",", trim: true)
  |> Enum.map(mapper)
end

batch_sizes = parse_csv.("BATCH_SIZES", "1000,10000", &String.to_integer/1)
event_types = parse_csv.("EVENT_TYPES", "log", &String.to_existing_atom/1)
shapes = parse_csv.("SHAPES", "realistic", &String.to_existing_atom/1)
benchee_time = System.get_env("BENCH_TIME", "5") |> String.to_integer()
benchee_warmup = System.get_env("BENCH_WARMUP", "2") |> String.to_integer()

inputs =
  for {shape, shape_idx} <- Enum.with_index(shapes),
      {type, type_idx} <- Enum.with_index(event_types),
      {batch_size, size_idx} <- Enum.with_index(batch_sizes),
      into: %{} do
    {compiled, config_id} = Data.compiled(type)
    events = Data.batch(type, batch_size, shape)
    backend_id = 9_000_000 + shape_idx * 10_000 + type_idx * 100 + size_idx
    {queue_key, queue_tid} = Data.setup_queue(backend_id)
    encoded_bytes = Data.encoded_bytes(events, type, compiled, config_id)

    label = "#{type}/#{shape}/batch=#{batch_size}"

    IO.puts(
      "#{label}: #{encoded_bytes} uncompressed RowBinary bytes " <>
        "(#{Float.round(encoded_bytes / batch_size, 1)} bytes/event)"
    )

    input = %{
      events: events,
      type: type,
      compiled: compiled,
      config_id: config_id,
      queue_key: queue_key,
      queue_tid: queue_tid
    }

    :ok = Data.validate_scenarios!(input)
    {label, input}
  end

IO.puts("")

Benchee.run(
  %{
    "old queue pop + encode_batch + gzip" => fn input ->
      Data.run_scenario(:old_queue, input)
    end,
    "id-passing queue + stream deflate" => fn input ->
      Data.run_scenario(:id_queue, input)
    end,
    "id-passing queue + stream deflate (hoisted uuid)" => fn input ->
      Data.run_scenario(:id_queue_hoisted, input)
    end,
    "id-passing queue + stream deflate (chunked)" => fn input ->
      Data.run_scenario(:id_queue_chunked, input)
    end,
    "id-passing queue + metadata routing + hoisted stream" => fn input ->
      Data.run_scenario(:id_queue_metadata, input)
    end
  },
  inputs: inputs,
  time: benchee_time,
  warmup: benchee_warmup,
  memory_time: System.get_env("BENCH_MEMORY_TIME", "2") |> String.to_integer(),
  reduction_time: System.get_env("BENCH_REDUCTION_TIME", "2") |> String.to_integer(),
  print: [configuration: false]
)
