# Pipeline throughput benchmark.
#
# Starts a self-contained Broadway pipeline using the real BufferProducer with
# the same concurrency settings as the BQ pipeline, but with a no-op handle_batch.
# Inserts N events into IngestEventQueue and measures time until all have been
# batched. No BigQuery credentials needed — comparable across branches.
#
# Run with:
#   BENCH_EVENTS=100000 mix run scripts/bench.exs

alias Logflare.Backends.BufferProducer
alias Logflare.Backends.IngestEventQueue
alias Logflare.LogEvent

target   = String.to_integer(System.get_env("BENCH_EVENTS", "100000"))
source_id = :bench_source
backend_id = nil

# ---------------------------------------------------------------------------
# Minimal stub source — only the fields BufferProducer needs at init.
# ---------------------------------------------------------------------------
source = %Logflare.Sources.Source{
  id: source_id,
  token: :bench_source_token,
  name: "bench",
  lock_schema: true,
  system_source: false,
  user_id: 0
}

# ---------------------------------------------------------------------------
# Bench pipeline — same producer/processor/batcher settings as the BQ pipeline,
# no-op handle_batch so we measure Broadway + BufferProducer overhead only.
# ---------------------------------------------------------------------------
defmodule BenchPipeline do
  use Broadway

  alias Logflare.Backends.BufferProducer

  def start_link(opts) do
    source   = Keyword.fetch!(opts, :source)
    sid_bid  = {source.id, nil}

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      hibernate_after: 5_000,
      spawn_opt: [fullsweep_after: 10],
      producer: [
        module: {BufferProducer, [source_id: source.id, backend_id: nil, id_passing: true]},
        transformer: {__MODULE__, :transform, [ref: {sid_bid, %{max_retries: 0}}]},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 8, max_demand: 100]
      ],
      batchers: [
        bq: [concurrency: 16, batch_size: 500, batch_timeout: 1_500, max_demand: 500]
      ],
      context: %{source_id: source.id, source_token: source.token,
                 counter: Keyword.fetch!(opts, :counter)}
    )
  end

  def transform(event, args) do
    %Broadway.Message{
      data: event,
      acknowledger: {__MODULE__, args[:ref], :ack_data}
    }
  end

  @impl Broadway.Acknowledger
  def ack(_ref, successful, _failed) do
    for %{data: {id, tid}} <- successful, do: :ets.delete(tid, id)
    :ok
  end

  @impl Broadway
  def handle_message(_proc, message, _ctx), do: Message.put_batcher(message, :bq)

  @impl Broadway
  def handle_batch(:bq, messages, batch_info, %{counter: counter}) do
    :atomics.add(counter, 1, batch_info.size)

    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
      %{backend_type: :bigquery}
    )

    messages
  end
end

# ---------------------------------------------------------------------------
# Bootstrap: ETS startup queue + pipeline
# ---------------------------------------------------------------------------
sid_bid_pid = {source_id, backend_id, nil}
{:ok, _tid} = IngestEventQueue.upsert_tid(sid_bid_pid)

alias Broadway.Message

{:ok, _pid} = BenchPipeline.start_link(
  source: source,
  counter: counter = :atomics.new(1, [])
)

# Wait for BufferProducer to register its live queue.
IO.write("Waiting for pipeline queue...")

Enum.reduce_while(Stream.repeatedly(fn -> Process.sleep(100) end), nil, fn _, _ ->
  live = IngestEventQueue.list_queues({source_id, backend_id})
         |> Enum.reject(fn {_, _, pid} -> is_nil(pid) end)
  if live != [], do: {:halt, :ok}, else: {:cont, nil}
end)

IO.puts(" ready.\n")

# ---------------------------------------------------------------------------
# Build and insert events
# ---------------------------------------------------------------------------
IO.puts("Building #{target} events...")

now_us = System.os_time(:microsecond)

events =
  Enum.map(1..target, fn i ->
    LogEvent.make(
      %{"message" => "bench #{i}", "timestamp" => now_us + i},
      %{source: source}
    )
  end)

IO.puts("Inserting #{target} events...")

start_ms = System.monotonic_time(:millisecond)
IngestEventQueue.add_to_table({source_id, backend_id}, events, check_queue_size: false)
IO.puts("Inserted. Draining...\n")

# ---------------------------------------------------------------------------
# Wait for all events to pass through handle_batch
# ---------------------------------------------------------------------------
Stream.repeatedly(fn -> Process.sleep(100) end)
|> Enum.reduce_while(nil, fn _, _ ->
  total = :atomics.get(counter, 1)

  IO.write("\r  processed=#{total}/#{target} (#{round(total / target * 100)}%)")

  if total >= target do
    elapsed_s = (System.monotonic_time(:millisecond) - start_ms) / 1_000
    avg_rate  = round(target / elapsed_s)

    IO.puts("\n\nDONE: #{target} events in #{Float.round(elapsed_s, 2)}s = #{avg_rate}/s avg")
    {:halt, :ok}
  else
    {:cont, nil}
  end
end)

Supervisor.stop(BenchPipeline)
