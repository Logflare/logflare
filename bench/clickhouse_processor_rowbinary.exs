# Fixed-work benchmark for moving fused ClickHouse mapping/RowBinary production
# from Broadway batch processors into processors.
#
# Examples:
#
#   MIX_ENV=test SCENARIO=baseline EVENT_TYPE=log BATCH_SIZE=10000 BATCHES=20 \
#     mix run --no-start bench/clickhouse_processor_rowbinary.exs
#
#   MIX_ENV=test SCENARIO=processor CONCURRENCY=6 EVENT_TYPE=log \
#     BATCH_SIZE=10000 BATCHES=20 \
#     mix run --no-start bench/clickhouse_processor_rowbinary.exs
#
# Scenarios:
#   baseline  ETS lookup -> fused mapper/RowBinary -> streaming gzip, serially
#   processor ETS lookup -> fused mapper/RowBinary in parallel processor chunks,
#             then streaming gzip serially
#   encode    processor-side lookup and fused mapper/RowBinary only
#   compress  gzip already-encoded rows only

Code.require_file("support/clickhouse_pipeline_bench_data.exs", __DIR__)

defmodule Logflare.Bench.ClickHouseProcessorRowBinary do
  @moduledoc false

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.LogEvent
  alias Logflare.Mapper
  alias Logflare.Mapper.OutputContext

  @spec encode_rows(
          [{term(), :ets.tid()}],
          reference(),
          binary(),
          pos_integer(),
          pos_integer()
        ) :: [binary()]
  def encode_rows(id_tid_pairs, compiled, mapping_config_id, concurrency, processor_chunk_size) do
    id_tid_pairs
    |> Enum.chunk_every(processor_chunk_size)
    |> Task.async_stream(
      fn chunk -> Enum.map(chunk, &encode_pointer(&1, compiled, mapping_config_id)) end,
      max_concurrency: concurrency,
      ordered: true,
      timeout: :infinity
    )
    |> Enum.flat_map(fn {:ok, rows} -> rows end)
  end

  @spec encode_rows_serial([{term(), :ets.tid()}], reference(), binary()) :: [binary()]
  def encode_rows_serial(id_tid_pairs, compiled, mapping_config_id) do
    Enum.map(id_tid_pairs, &encode_pointer(&1, compiled, mapping_config_id))
  end

  @spec compress_rows([binary()]) :: binary()
  def compress_rows(rows) do
    gzip_stream(fn z -> Enum.map(rows, &:zlib.deflate(z, &1)) end)
  end

  @spec baseline([{term(), :ets.tid()}], reference(), binary()) :: binary()
  def baseline(id_tid_pairs, compiled, mapping_config_id) do
    gzip_stream(fn z ->
      Enum.map(id_tid_pairs, fn pair ->
        :zlib.deflate(z, encode_pointer(pair, compiled, mapping_config_id))
      end)
    end)
  end

  defp encode_pointer({id, tid}, compiled, mapping_config_id) do
    %LogEvent{} = event = IngestEventQueue.lookup_event(tid, id)
    output_context = OutputContext.clickhouse_row_binary(event, mapping_config_id)
    Mapper.map(event.body, compiled, output_context: output_context)
  end

  defp gzip_stream(fun) do
    z = :zlib.open()

    try do
      :zlib.deflateInit(z, :default, :deflated, 31, 8, :default)
      chunks = fun.(z)
      IO.iodata_to_binary([chunks, :zlib.deflate(z, "", :finish)])
    after
      :zlib.deflateEnd(z)
      :zlib.close(z)
    end
  end
end

alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester
alias Logflare.Bench.ClickHousePipelineData, as: Data
alias Logflare.Bench.ClickHouseProcessorRowBinary, as: Bench

scenario = System.get_env("SCENARIO", "processor") |> String.to_existing_atom()
type = System.get_env("EVENT_TYPE", "log") |> String.to_existing_atom()
batch_size = System.get_env("BATCH_SIZE", "10000") |> String.to_integer()
batches = System.get_env("BATCHES", "20") |> String.to_integer()
warmup_batches = System.get_env("WARMUP_BATCHES", "2") |> String.to_integer()
concurrency = System.get_env("CONCURRENCY", "6") |> String.to_integer()
processor_chunk_size = System.get_env("PROCESSOR_CHUNK_SIZE", "1000") |> String.to_integer()

{compiled, config_id} = Data.compiled_output(type)
mapping_config_id = Ingester.encode_mapping_config_id(config_id)
events = Data.batch(type, batch_size, :realistic)
tid = Data.setup_processing_ets(events)
id_tid_pairs = Enum.map(events, &{&1.id, tid})
encoded_rows = Bench.encode_rows_serial(id_tid_pairs, compiled, mapping_config_id)

baseline = Bench.baseline(id_tid_pairs, compiled, mapping_config_id)
candidate = Bench.compress_rows(encoded_rows)

if baseline != candidate do
  raise "processor-side output differs from the batch-side baseline"
end

run =
  case scenario do
    :baseline ->
      fn -> Bench.baseline(id_tid_pairs, compiled, mapping_config_id) end

    :processor ->
      fn ->
        id_tid_pairs
        |> Bench.encode_rows(compiled, mapping_config_id, concurrency, processor_chunk_size)
        |> Bench.compress_rows()
      end

    :encode ->
      fn ->
        Bench.encode_rows(
          id_tid_pairs,
          compiled,
          mapping_config_id,
          concurrency,
          processor_chunk_size
        )
      end

    :compress ->
      fn -> Bench.compress_rows(encoded_rows) end
  end

if warmup_batches > 0 do
  for _ <- 1..warmup_batches, do: run.()
end

:erlang.garbage_collect()
:erlang.statistics(:runtime)
:erlang.statistics(:wall_clock)
:erlang.statistics(:reductions)

{elapsed_us, result_bytes} =
  :timer.tc(fn ->
    Enum.reduce(1..batches, 0, fn _, total ->
      result = run.()
      bytes = if is_binary(result), do: byte_size(result), else: Enum.sum_by(result, &byte_size/1)
      total + bytes
    end)
  end)

{_, runtime_ms} = :erlang.statistics(:runtime)
{_, wall_ms} = :erlang.statistics(:wall_clock)
{_, reductions} = :erlang.statistics(:reductions)
rows = batch_size * batches

IO.puts("scenario=#{scenario}")
IO.puts("event_type=#{type}")
IO.puts("schedulers_online=#{System.schedulers_online()}")
IO.puts("concurrency=#{concurrency}")
IO.puts("processor_chunk_size=#{processor_chunk_size}")
IO.puts("batch_size=#{batch_size}")
IO.puts("batches=#{batches}")
IO.puts("rows=#{rows}")

IO.puts(
  "rowbinary_bytes_per_row=#{Float.round(Enum.sum_by(encoded_rows, &byte_size/1) / batch_size, 2)}"
)

IO.puts("result_bytes_total=#{result_bytes}")
IO.puts("elapsed_us=#{elapsed_us}")
IO.puts("runtime_ms=#{runtime_ms}")
IO.puts("wall_ms=#{wall_ms}")
IO.puts("reductions=#{reductions}")
IO.puts("rows_per_wall_second=#{Float.round(rows / (elapsed_us / 1_000_000), 2)}")
IO.puts("wall_us_per_row=#{Float.round(elapsed_us / rows, 4)}")
IO.puts("runtime_us_per_row=#{Float.round(runtime_ms * 1000 / rows, 4)}")
IO.puts("reductions_per_row=#{Float.round(reductions / rows, 2)}")
