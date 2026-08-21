# credo:disable-for-this-file Credo.Check.Refactor.IoPuts
#
# Usage:
#   MIX_ENV=test mix run test/profiling/ingest_event_queue_pointer_claim_bench.exs
#
# Measures the two production queue hot paths changed by the pointer-claim optimization
# with a full 500-event batch. The claim job restores the pointer rows inside each timed
# invocation so every iteration can atomically claim the same batch. That shared restore
# work makes the measured claim improvement conservative.
#
# Recorded in the same six-scheduler Linux container with BENCH_TIME=2, comparing
# parent 825bd40d to the optimized implementation:
#
#   job                         median (old -> new)   memory (old -> new)    reductions
#   claim + restore pointers     128.57 -> 99.05 us   200.78 -> 187.51 KB   6.12 -> 3.89 K
#   resolve generation events     66.62 -> 61.63 us   486.17 -> 456.48 KB   5.61 -> 5.56 K

alias Logflare.Backends.IngestEventQueue
alias Logflare.Factory

batch_size = 500
bench_time = System.get_env("BENCH_TIME", "3") |> String.to_integer()

user = Factory.insert(:user)
source = Factory.insert(:source, user: user)
queue = {source.id, nil, self()}
IngestEventQueue.upsert_tid(queue)

events = for _ <- 1..batch_size, do: Factory.build(:log_event, source: source)
:ok = IngestEventQueue.add_to_table(queue, events)
{:ok, pointers, queue_tid} = IngestEventQueue.pop_pending_pointers(queue, batch_size)

pointer_rows =
  Enum.map(pointers, fn pointer ->
    {pointer.id, pointer.tid, pointer.gen_event_id, pointer.size, pointer.retries,
     pointer.event_type, pointer.day_bucket}
  end)

Benchee.run(
  %{
    "production: claim + restore 500 pointers" => fn ->
      true = :ets.insert(queue_tid, pointer_rows)
      {:ok, claimed, ^queue_tid} = IngestEventQueue.pop_pending_pointers(queue, batch_size)
      ^batch_size = length(claimed)
    end,
    "production: resolve 500 generation events" => fn ->
      Enum.each(pointers, fn pointer ->
        %Logflare.LogEvent{} =
          IngestEventQueue.lookup_event(pointer.tid, pointer.gen_event_id)
      end)
    end
  },
  time: bench_time,
  warmup: bench_time,
  memory_time: bench_time,
  reduction_time: bench_time
)
