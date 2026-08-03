# Fixed-work CPU runner for PR #3667's ClickHouse pipeline changes.
#
# Benchee is useful for local comparisons, but this script is intended to answer
# the CPU claim directly: for the same number of rows, how many CPU seconds does
# each path consume? Run one scenario per OS process and compare user+sys time.
#
# Linux:
#   /usr/bin/time -v env SCENARIO=old BATCHES=100 BATCH_SIZE=10000 \
#     mix run --no-start bench/clickhouse_pipeline_cpu_runner.exs
#   /usr/bin/time -v env SCENARIO=stream_ets BATCHES=100 BATCH_SIZE=10000 \
#     mix run --no-start bench/clickhouse_pipeline_cpu_runner.exs
#
# macOS:
#   /usr/bin/time -lp env SCENARIO=old BATCHES=100 BATCH_SIZE=10000 \
#     mix run --no-start bench/clickhouse_pipeline_cpu_runner.exs
#
# Scenarios:
#   old              map all -> encode_batch -> gzip
#   old_materialized map all -> encode_batch -> iodata_to_binary -> gzip
#   stream           per-row encode_row -> incremental deflate
#   stream_hoisted   stream with mapping_config_id UUID encoded once per batch
#   stream_chunked   stream_hoisted plus deflate every CHUNK_ROWS rows
#   stream_ets       stream plus per-row ETS lookup
#   stream_ets_*     ETS variants of hoisted/chunked stream scenarios
#   old_queue        add_to_table -> pop_pending -> old path
#   id_queue         add_to_table -> take_pending_ids -> lookup -> stream -> delete_id
#   id_queue_*       queue variants of hoisted/chunked stream scenarios
#   id_queue_metadata optimized queue path with routing metadata + hoisted stream
#
# Compare CPU normalized by rows. Higher CPU% alone is not evidence of higher
# cost if wall time changes; user+sys CPU seconds per row is the primary signal.

Code.require_file("support/clickhouse_pipeline_bench_data.exs", __DIR__)

alias Logflare.Bench.ClickHousePipelineData, as: Data

scenario =
  case System.get_env("SCENARIO", "old") do
    "old" -> :old
    "old_materialized" -> :old_materialized
    "stream" -> :stream
    "stream_hoisted" -> :stream_hoisted
    "stream_chunked" -> :stream_chunked
    "stream_ets" -> :stream_ets
    "stream_ets_hoisted" -> :stream_ets_hoisted
    "stream_ets_chunked" -> :stream_ets_chunked
    "old_queue" -> :old_queue
    "id_queue" -> :id_queue
    "id_queue_hoisted" -> :id_queue_hoisted
    "id_queue_chunked" -> :id_queue_chunked
    "id_queue_metadata" -> :id_queue_metadata
    other -> raise ArgumentError, "unknown SCENARIO=#{inspect(other)}"
  end

type = System.get_env("EVENT_TYPE", "log") |> String.to_existing_atom()
shape = System.get_env("SHAPE", "realistic") |> String.to_existing_atom()
batch_size = System.get_env("BATCH_SIZE", "10000") |> String.to_integer()
batches = System.get_env("BATCHES", "100") |> String.to_integer()
warmup_batches = System.get_env("WARMUP_BATCHES", "3") |> String.to_integer()

{compiled, config_id} = Data.compiled(type)
events = Data.batch(type, batch_size, shape)
encoded_bytes = Data.encoded_bytes(events, type, compiled, config_id)

input = %{
  events: events,
  type: type,
  compiled: compiled,
  config_id: config_id
}

input =
  if scenario in [:stream_ets, :stream_ets_hoisted, :stream_ets_chunked] do
    Map.put(input, :processing_tid, Data.setup_processing_ets(events))
  else
    input
  end

input =
  if scenario in [:old_queue, :id_queue, :id_queue_hoisted, :id_queue_chunked, :id_queue_metadata] do
    {queue_key, queue_tid} = Data.setup_queue(8_888_888)

    input
    |> Map.put(:queue_key, queue_key)
    |> Map.put(:queue_tid, queue_tid)
  else
    input
  end

IO.puts("scenario=#{scenario}")
IO.puts("event_type=#{type}")
IO.puts("shape=#{shape}")
IO.puts("batch_size=#{batch_size}")
IO.puts("batches=#{batches}")
IO.puts("rows=#{batch_size * batches}")
IO.puts("uncompressed_rowbinary_bytes_per_batch=#{encoded_bytes}")
IO.puts("uncompressed_rowbinary_bytes_per_row=#{Float.round(encoded_bytes / batch_size, 2)}")

:ok = Data.validate_scenario!(scenario, input)
IO.puts("validation=ok")

if warmup_batches > 0 do
  for _ <- 1..warmup_batches do
    Data.run_scenario(scenario, input) |> byte_size()
  end
end

:erlang.garbage_collect()
:erlang.statistics(:runtime)
:erlang.statistics(:wall_clock)
:erlang.statistics(:reductions)

{elapsed_us, compressed_bytes} =
  :timer.tc(fn ->
    Enum.reduce(1..batches, 0, fn _, acc ->
      acc + (Data.run_scenario(scenario, input) |> byte_size())
    end)
  end)

{_, runtime_ms} = :erlang.statistics(:runtime)
{_, wall_ms} = :erlang.statistics(:wall_clock)
{_, reductions} = :erlang.statistics(:reductions)

rows = batch_size * batches

IO.puts("compressed_bytes_total=#{compressed_bytes}")
IO.puts("elapsed_us=#{elapsed_us}")
IO.puts("runtime_ms=#{runtime_ms}")
IO.puts("wall_ms=#{wall_ms}")
IO.puts("reductions=#{reductions}")
IO.puts("rows_per_wall_second=#{Float.round(rows / (elapsed_us / 1_000_000), 2)}")
IO.puts("wall_us_per_row=#{Float.round(elapsed_us / rows, 4)}")
IO.puts("runtime_us_per_row=#{Float.round(runtime_ms * 1000 / rows, 4)}")
IO.puts("reductions_per_row=#{Float.round(reductions / rows, 2)}")
