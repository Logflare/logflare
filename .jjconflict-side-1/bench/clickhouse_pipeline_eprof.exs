# eprof profiler for ClickHouse encode/compress pipeline scenarios.
#
# This is intentionally separate from the Benchee scripts: it answers "where is
# the CPU going?" rather than "which scenario is faster?". It profiles fixed
# work in a single BEAM process and prints eprof's total-time table.
#
# Usage:
#
#   SCENARIO=old BATCH_SIZE=1000 ITERATIONS=1 \
#     mix run --no-start bench/clickhouse_pipeline_eprof.exs
#   SCENARIO=stream_ets BATCH_SIZE=10 ITERATIONS=100 \
#     mix run --no-start bench/clickhouse_pipeline_eprof.exs
#
# Scenarios are the same as clickhouse_pipeline_cpu_runner.exs:
#   old, old_materialized, stream, stream_hoisted, stream_chunked,
#   stream_ets, stream_ets_hoisted, stream_ets_chunked,
#   old_queue, id_queue, id_queue_hoisted, id_queue_chunked, id_queue_metadata

Code.require_file("support/clickhouse_pipeline_bench_data.exs", __DIR__)

alias Logflare.Bench.ClickHousePipelineData, as: Data

scenario = System.get_env("SCENARIO", "old") |> String.to_existing_atom()
type = System.get_env("EVENT_TYPE", "log") |> String.to_existing_atom()
shape = System.get_env("SHAPE", "small") |> String.to_existing_atom()
batch_size = System.get_env("BATCH_SIZE", "1000") |> String.to_integer()
iterations = System.get_env("ITERATIONS", "1") |> String.to_integer()

{compiled, config_id} = Data.compiled(type)
events = Data.batch(type, batch_size, shape)

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
    {queue_key, queue_tid} = Data.setup_queue(7_777_777)

    input
    |> Map.put(:queue_key, queue_key)
    |> Map.put(:queue_tid, queue_tid)
  else
    input
  end

Data.run_scenario(scenario, input) |> byte_size()
:erlang.garbage_collect()

IO.puts(
  "scenario=#{scenario} event_type=#{type} shape=#{shape} " <>
    "batch_size=#{batch_size} iterations=#{iterations}"
)

:eprof.start()

:eprof.profile(fn ->
  for _ <- 1..iterations do
    Data.run_scenario(scenario, input) |> byte_size()
  end
end)

:eprof.stop_profiling()
:eprof.analyze(:total)
:eprof.stop()
