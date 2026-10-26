# credo:disable-for-this-file Credo.Check.Refactor.IoPuts
#
# Usage:
#   MIX_ENV=test mix run --no-start test/profiling/bq_schema_admission_bench.exs
#
# Optional env:
#   MESSAGES=50000 FIELDS=50 LIMIT=8

alias Logflare.Sources.Source.BigQuery.Schema
alias Logflare.Sources.Source.BigQuery.SchemaMetrics

messages = String.to_integer(System.get_env("MESSAGES") || "50000")
fields = String.to_integer(System.get_env("FIELDS") || "50")
limit = String.to_integer(System.get_env("LIMIT") || "8")

body = Map.new(1..fields, &{"field_#{&1}", String.duplicate("value", 4)})
message = {:update, %{body: body, id: "benchmark"}, %{lock_schema: false}}

run = fn mode ->
  {:ok, sink} = Agent.start(fn -> nil end)
  :ok = :sys.suspend(sink)
  counter = :atomics.new(1, [])

  {elapsed_us, admitted} =
    :timer.tc(fn ->
      Enum.reduce(1..messages, 0, fn _, admitted ->
        reservation =
          if mode == :unbounded,
            do: :ok,
            else: Schema.reserve_update_slot(counter, limit)

        if reservation == :ok do
          GenServer.cast(sink, message)
          admitted + 1
        else
          admitted
        end
      end)
    end)

  {:message_queue_len, queue_length} = Process.info(sink, :message_queue_len)
  {:memory, memory_bytes} = Process.info(sink, :memory)
  Process.exit(sink, :kill)

  %{
    mode: mode,
    elapsed_us: elapsed_us,
    admitted: admitted,
    queue_length: queue_length,
    memory_bytes: memory_bytes
  }
end

unbounded = run.(:unbounded)
bounded = run.(:bounded)

full_counter = :atomics.new(1, [])
:atomics.put(full_counter, 1, limit)

{rejection_us, _result} =
  :timer.tc(fn ->
    Enum.each(1..messages, fn _ ->
      :full = Schema.reserve_update_slot(full_counter, limit)
    end)
  end)

accepted_counter = :atomics.new(1, [])

{accepted_us, _result} =
  :timer.tc(fn ->
    Enum.each(1..messages, fn _ ->
      :ok = Schema.reserve_update_slot(accepted_counter, limit)
      :atomics.sub(accepted_counter, 1, 1)
    end)
  end)

{:ok, metrics_pid} = SchemaMetrics.start_link(nil)

{instrumented_rejection_us, _result} =
  :timer.tc(fn ->
    Enum.each(1..messages, fn _ ->
      SchemaMetrics.record_sample(:zero_rate)
      :full = Schema.reserve_update_slot(full_counter, limit)
      SchemaMetrics.record_admission(:rejected)
    end)
  end)

GenServer.stop(metrics_pid)

format_mb = &Float.round(&1 / 1_048_576, 2)

result = %{
  inputs: %{messages: messages, fields: fields, limit: limit},
  unbounded: Map.put(unbounded, :memory_mb, format_mb.(unbounded.memory_bytes)),
  bounded: Map.put(bounded, :memory_mb, format_mb.(bounded.memory_bytes)),
  reductions: %{
    queue: Float.round(unbounded.queue_length / max(bounded.queue_length, 1), 2),
    memory: Float.round(unbounded.memory_bytes / max(bounded.memory_bytes, 1), 2)
  },
  saturated_gate_ns_per_attempt: Float.round(rejection_us * 1_000 / messages, 2),
  accepted_gate_ns_per_cycle: Float.round(accepted_us * 1_000 / messages, 2),
  instrumented_rejection_ns_per_attempt:
    Float.round(instrumented_rejection_us * 1_000 / messages, 2)
}

IO.puts(inspect(result, pretty: true))
