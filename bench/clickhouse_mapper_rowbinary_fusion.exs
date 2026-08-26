# Compares the production ClickHouse ETS -> mapper -> RowBinary -> gzip path
# with fused native mapping/encoding performed one row at a time.
#
#   MIX_ENV=test mix run --no-start bench/clickhouse_mapper_rowbinary_fusion.exs
#   BENCH_SECTION=encoding EVENT_TYPES=log BATCH_SIZES=1000 BENCH_TIME=5 \
#     MIX_ENV=test mix run --no-start bench/clickhouse_mapper_rowbinary_fusion.exs
#   FIXED_SCENARIO=fused FIXED_BATCHES=100 EVENT_TYPES=log BATCH_SIZES=1000 \
#     MIX_ENV=test mix run --no-start bench/clickhouse_mapper_rowbinary_fusion.exs

Code.require_file("support/clickhouse_pipeline_bench_data.exs", __DIR__)

defmodule Logflare.Bench.ClickHouseMapperRowBinaryFusion do
  @moduledoc false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.LogEvent
  alias Logflare.Mapper
  alias Logflare.Mapper.OutputContext

  @spec separate_row(LogEvent.t(), atom(), reference(), binary()) :: iodata()
  def separate_row(%LogEvent{} = event, type, compiled, mapping_config_id) do
    mapped_body =
      event.body
      |> Mapper.map(compiled)
      |> maybe_compute_duration(type)
      |> resolve_severity_number(type)

    Ingester.encode_row(%{event | body: mapped_body}, type, mapping_config_id)
  end

  @spec encode_separate([LogEvent.t()], atom(), reference(), binary()) :: binary()
  def encode_separate(events, type, compiled, mapping_config_id) do
    events
    |> Enum.map(&separate_row(&1, type, compiled, mapping_config_id))
    |> IO.iodata_to_binary()
  end

  @spec encode_fused([LogEvent.t()], atom(), reference(), binary()) :: binary()
  def encode_fused(events, _type, compiled, mapping_config_id) do
    events
    |> Enum.map(fn event ->
      output_context = OutputContext.ch_row_binary(event, mapping_config_id)
      Mapper.map(event.body, compiled, output_context: output_context)
    end)
    |> IO.iodata_to_binary()
  end

  @spec compress_ets(
          [{term(), :ets.tid()}],
          atom(),
          reference(),
          binary(),
          :separate | :fused
        ) :: binary()
  def compress_ets(id_tid_pairs, type, compiled, mapping_config_id, mode) do
    z = :zlib.open()

    try do
      :zlib.deflateInit(z, :default, :deflated, 31, 8, :default)
      chunks = encode_ets_chunks(z, id_tid_pairs, type, compiled, mapping_config_id, mode)
      IO.iodata_to_binary([chunks, :zlib.deflate(z, "", :finish)])
    after
      :zlib.deflateEnd(z)
      :zlib.close(z)
    end
  end

  defp encode_ets_chunks(z, id_tid_pairs, type, compiled, mapping_config_id, :separate) do
    Enum.map(id_tid_pairs, fn {id, tid} ->
      case IngestEventQueue.lookup_event(tid, id) do
        %LogEvent{} = event ->
          :zlib.deflate(z, separate_row(event, type, compiled, mapping_config_id))

        nil ->
          []
      end
    end)
  end

  defp encode_ets_chunks(z, id_tid_pairs, _type, compiled, mapping_config_id, :fused) do
    Enum.map(id_tid_pairs, fn {id, tid} ->
      case IngestEventQueue.lookup_event(tid, id) do
        %LogEvent{} = event ->
          output_context = OutputContext.ch_row_binary(event, mapping_config_id)
          row = Mapper.map(event.body, compiled, output_context: output_context)
          :zlib.deflate(z, row)

        nil ->
          []
      end
    end)
  end

  defp maybe_compute_duration(
         %{"start_time" => start_time, "end_time" => end_time, "duration" => 0} = body,
         :trace
       )
       when is_integer(start_time) and is_integer(end_time) and end_time > start_time do
    %{body | "duration" => end_time - start_time}
  end

  defp maybe_compute_duration(body, _type), do: body

  defp resolve_severity_number(%{"severity_number_alt" => alt} = body, :log)
       when is_integer(alt) and alt > 0 do
    %{body | "severity_number" => alt}
  end

  defp resolve_severity_number(body, _type), do: body
end

alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester
alias Logflare.Bench.ClickHouseMapperRowBinaryFusion, as: Fusion
alias Logflare.Bench.ClickHousePipelineData, as: Data

parse_csv = fn name, default, mapper ->
  name
  |> System.get_env(default)
  |> String.split(",", trim: true)
  |> Enum.map(mapper)
end

event_types = parse_csv.("EVENT_TYPES", "log,metric,trace", &String.to_existing_atom/1)
batch_sizes = parse_csv.("BATCH_SIZES", "1000", &String.to_integer/1)

inputs =
  for type <- event_types,
      batch_size <- batch_sizes,
      into: %{} do
    {map_compiled, config_id} = Data.compiled(type)
    {output_compiled, ^config_id} = Data.compiled_output(type)
    mapping_config_id = Ingester.encode_mapping_config_id(config_id)
    events = Data.batch(type, batch_size, :realistic)
    processing_tid = Data.setup_processing_ets(events)
    id_tid_pairs = Enum.map(events, &{&1.id, processing_tid})

    expected = Fusion.encode_separate(events, type, map_compiled, mapping_config_id)
    actual = Fusion.encode_fused(events, type, output_compiled, mapping_config_id)

    if actual != expected do
      raise "fused RowBinary differs for #{type}/batch=#{batch_size}"
    end

    IO.puts("#{type}/batch=#{batch_size}: validation=ok bytes=#{byte_size(expected)}")

    {"#{type}/batch=#{batch_size}",
     %{
       events: events,
       type: type,
       map_compiled: map_compiled,
       output_compiled: output_compiled,
       mapping_config_id: mapping_config_id,
       id_tid_pairs: id_tid_pairs
     }}
  end

pipeline_scenarios = %{
  "fused map + RowBinary" => fn input ->
    Fusion.compress_ets(
      input.id_tid_pairs,
      input.type,
      input.output_compiled,
      input.mapping_config_id,
      :fused
    )
  end,
  "separate map + RowBinary" => fn input ->
    Fusion.compress_ets(
      input.id_tid_pairs,
      input.type,
      input.map_compiled,
      input.mapping_config_id,
      :separate
    )
  end
}

encoding_scenarios = %{
  "fused map + RowBinary" => fn input ->
    Fusion.encode_fused(
      input.events,
      input.type,
      input.output_compiled,
      input.mapping_config_id
    )
  end,
  "separate map + RowBinary" => fn input ->
    Fusion.encode_separate(
      input.events,
      input.type,
      input.map_compiled,
      input.mapping_config_id
    )
  end
}

scenarios =
  case System.get_env("BENCH_SECTION", "pipeline") do
    "pipeline" -> pipeline_scenarios
    "encoding" -> encoding_scenarios
    section -> raise ArgumentError, "unknown BENCH_SECTION=#{inspect(section)}"
  end

case System.get_env("FIXED_SCENARIO") do
  nil ->
    Benchee.run(
      scenarios,
      inputs: inputs,
      time: System.get_env("BENCH_TIME", "5") |> String.to_integer(),
      warmup: System.get_env("BENCH_WARMUP", "2") |> String.to_integer(),
      memory_time: System.get_env("BENCH_MEMORY_TIME", "2") |> String.to_integer(),
      reduction_time: System.get_env("BENCH_REDUCTION_TIME", "2") |> String.to_integer(),
      print: [configuration: false]
    )

  scenario when scenario in ["separate", "fused"] ->
    [input] = Map.values(inputs)
    batches = System.get_env("FIXED_BATCHES", "100") |> String.to_integer()
    warmup_batches = System.get_env("FIXED_WARMUP_BATCHES", "3") |> String.to_integer()

    run =
      case scenario do
        "separate" ->
          fn ->
            Fusion.compress_ets(
              input.id_tid_pairs,
              input.type,
              input.map_compiled,
              input.mapping_config_id,
              :separate
            )
          end

        "fused" ->
          fn ->
            Fusion.compress_ets(
              input.id_tid_pairs,
              input.type,
              input.output_compiled,
              input.mapping_config_id,
              :fused
            )
          end
      end

    for _ <- 1..warmup_batches, do: run.() |> byte_size()

    :erlang.garbage_collect()
    :erlang.statistics(:runtime)
    :erlang.statistics(:wall_clock)
    :erlang.statistics(:reductions)

    {elapsed_us, compressed_bytes} =
      :timer.tc(fn ->
        Enum.reduce(1..batches, 0, fn _, total -> total + (run.() |> byte_size()) end)
      end)

    {_, runtime_ms} = :erlang.statistics(:runtime)
    {_, wall_ms} = :erlang.statistics(:wall_clock)
    {_, reductions} = :erlang.statistics(:reductions)
    rows = length(input.id_tid_pairs) * batches

    IO.puts("scenario=#{scenario}")
    IO.puts("event_type=#{input.type}")
    IO.puts("batches=#{batches}")
    IO.puts("rows=#{rows}")
    IO.puts("compressed_bytes_total=#{compressed_bytes}")
    IO.puts("elapsed_us=#{elapsed_us}")
    IO.puts("runtime_ms=#{runtime_ms}")
    IO.puts("wall_ms=#{wall_ms}")
    IO.puts("reductions=#{reductions}")
    IO.puts("wall_us_per_row=#{Float.round(elapsed_us / rows, 4)}")
    IO.puts("runtime_us_per_row=#{Float.round(runtime_ms * 1000 / rows, 4)}")
    IO.puts("reductions_per_row=#{Float.round(reductions / rows, 2)}")

  scenario ->
    raise ArgumentError, "unknown FIXED_SCENARIO=#{inspect(scenario)}"
end
