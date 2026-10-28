# Compares the steady-state and peak VM memory of claimed ClickHouse events backed
# by full LogEvents versus processor-produced RowBinary messages whose generation
# rows have been replaced with the same reference-counted binaries.
#
# Run each scenario in a separate OS process, preferably under /usr/bin/time -v:
#
#   MIX_ENV=test SCENARIO=event BATCH_SIZE=60000 \
#     mix run --no-start bench/clickhouse_processor_rowbinary_memory.exs
#   MIX_ENV=test SCENARIO=encoded BATCH_SIZE=60000 CONCURRENCY=6 \
#     mix run --no-start bench/clickhouse_processor_rowbinary_memory.exs

Code.require_file("support/clickhouse_pipeline_bench_data.exs", __DIR__)

alias Logflare.Backends.Adaptor.ClickHouseAdaptor.EncodedRow
alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester
alias Logflare.Backends.IngestEventQueue
alias Logflare.Backends.IngestEventQueue.LogEventPointer
alias Logflare.Bench.ClickHousePipelineData, as: Data
alias Logflare.LogEvent
alias Logflare.Mapper
alias Logflare.Mapper.OutputContext

scenario = System.get_env("SCENARIO", "event") |> String.to_existing_atom()
type = System.get_env("EVENT_TYPE", "log") |> String.to_existing_atom()
batch_size = System.get_env("BATCH_SIZE", "60000") |> String.to_integer()
concurrency = System.get_env("CONCURRENCY", "6") |> String.to_integer()
processor_chunk_size = System.get_env("PROCESSOR_CHUNK_SIZE", "1000") |> String.to_integer()

build_generation = fn ->
  events = Data.batch(type, batch_size, :realistic)
  tid = Data.setup_processing_ets(events)

  pointers =
    Enum.map(events, fn event ->
      %LogEventPointer{
        id: event.id,
        tid: tid,
        gen_event_id: event.id,
        queue_tid: tid,
        size: :erlang.external_size(event.body),
        retries: 0,
        event_type: event.event_type,
        day_bucket: event.day_bucket
      }
    end)

  {tid, pointers}
end

{tid, pointers} = build_generation.()
:erlang.garbage_collect()

held =
  case scenario do
    :event ->
      pointers

    :encoded ->
      {compiled, config_id} = Data.compiled_output(type)
      mapping_config_id = Ingester.encode_mapping_config_id(config_id)

      pointers
      |> Enum.chunk_every(processor_chunk_size)
      |> Task.async_stream(
        fn chunk ->
          Enum.map(chunk, fn pointer ->
            %LogEvent{} =
              event =
              IngestEventQueue.lookup_event(pointer.tid, pointer.gen_event_id)

            output_context = OutputContext.ch_row_binary(event, mapping_config_id)

            encoded = %EncodedRow{
              pointer: pointer,
              row: Mapper.map(event.body, compiled, output_context: output_context)
            }

            :ok =
              IngestEventQueue.replace_event(pointer.tid, pointer.gen_event_id, encoded)

            encoded
          end)
        end,
        max_concurrency: concurrency,
        ordered: true,
        timeout: :infinity
      )
      |> Enum.flat_map(fn {:ok, rows} -> rows end)
  end

:erlang.garbage_collect()
Process.sleep(100)

memory = :erlang.memory()

held_payload_bytes =
  Enum.sum_by(held, fn
    %EncodedRow{row: row} -> byte_size(row)
    %LogEventPointer{size: size} -> size
  end)

IO.puts("scenario=#{scenario}")
IO.puts("event_type=#{type}")
IO.puts("batch_size=#{batch_size}")
IO.puts("concurrency=#{concurrency}")
IO.puts("generation_words=#{:ets.info(tid, :memory)}")
IO.puts("held_payload_bytes=#{held_payload_bytes}")

for key <- [:total, :processes_used, :binary, :ets] do
  IO.puts("#{key}_bytes=#{Keyword.fetch!(memory, key)}")
end
