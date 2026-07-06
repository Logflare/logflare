alias Logflare.Backends.IngestEventQueue

# Create a test user and source
user = Logflare.Factory.insert(:user)
source = Logflare.Factory.insert(:source, user: user)

# Single-consumer queue, keyed by an owner pid (mirrors BufferProducer's {sid, bid, self()}).
owner = spawn(fn -> :timer.sleep(:infinity) end)
key = {source.id, nil, owner}
IngestEventQueue.upsert_tid(key)
tid = IngestEventQueue.get_tid(key)

# The row tuple is now 5 elements: {event_id, status, event, size, claim}. This match spec
# selects pending rows as {event_id, size}, mirroring the module's take_pending_ids/2.
select_ms = [{{:"$1", :pending, :"$2", :"$3", :"$4"}, [], [{{:"$1", :"$3"}}]}]

# Strategy A — per-id conditional CAS (the pre-update_counter implementation, #3609).
take_via_select_replace = fn tid, n ->
  case :ets.select(tid, select_ms, n) do
    {taken_pairs, _cont} ->
      Enum.filter(taken_pairs, fn {id, _size} ->
        replace_ms = [
          {{id, :pending, :"$1", :"$2", :"$3"}, [], [{{id, :processing, :"$1", :"$2", :"$3"}}]}
        ]

        :ets.select_replace(tid, replace_ms) == 1
      end)

    :"$end_of_table" ->
      []
  end
end

# Strategy B — dedup + unconditional update_element (the #3600 form; valid only for
# single-consumer queues). Cheaper: no per-id match-spec compilation.
take_via_update_element = fn tid, n ->
  case :ets.select(tid, select_ms, n) do
    {taken_pairs, _cont} ->
      taken_pairs
      |> Enum.uniq_by(fn {id, _size} -> id end)
      |> Enum.filter(fn {id, _size} -> :ets.update_element(tid, id, {2, :processing}) end)

    :"$end_of_table" ->
      []
  end
end

# Strategy D — single select_replace compiled once: one match spec with one literal-key
# clause per id, so ETS compiles the whole spec a single time (vs once per id) while
# keeping the set-table key index. No schema change, still multi-consumer safe. Caveat:
# select_replace returns only a count, so it cannot say *which* ids it won — the fast path
# assumes count == candidates (true with no contention). The single-threaded bench never
# hits the contention fallback. NOTE: this measures table_size == n; if ETS full-scans an
# N-clause spec instead of doing N keyed lookups, a large-table/small-n sweep would expose it.
take_via_batched_select_replace = fn tid, n ->
  case :ets.select(tid, select_ms, n) do
    {taken_pairs, _cont} ->
      candidates = Enum.uniq_by(taken_pairs, fn {id, _size} -> id end)

      replace_ms =
        for {id, _size} <- candidates do
          {{id, :pending, :"$1", :"$2", :"$3"}, [], [{{id, :processing, :"$1", :"$2", :"$3"}}]}
        end

      case replace_ms do
        [] ->
          []

        _ ->
          # In production, count < length(candidates) needs a per-id fallback to identify
          # the winners; never triggered single-threaded, so we return candidates directly.
          _claimed = :ets.select_replace(tid, replace_ms)
          candidates
      end

    :"$end_of_table" ->
      []
  end
end

# Strategy C — per-id update_counter (CAS-lite). Atomic and isolated under write_concurrency,
# but uses a direct integer op rather than a per-id match spec. The `0 -> 1` winner claims;
# `2+` means another consumer (or a resize-duplicate) already took it. No uniq_by needed.
# `update_counter` raises on a missing key (janitor delete race), so claim losses on that
# path surface as ArgumentError -> not won, mirroring update_element returning false.
take_via_update_counter = fn tid, n ->
  case :ets.select(tid, select_ms, n) do
    {taken_pairs, _cont} ->
      Enum.filter(taken_pairs, fn {id, _size} ->
        try do
          if :ets.update_counter(tid, id, {5, 1}) == 1 do
            :ets.update_element(tid, id, {2, :processing})
            true
          else
            false
          end
        rescue
          ArgumentError -> false
        end
      end)

    :"$end_of_table" ->
      []
  end
end

inputs = %{
  "250 events" => for(_ <- 1..250, do: Logflare.Factory.build(:log_event)),
  "1000 events" => for(_ <- 1..1_000, do: Logflare.Factory.build(:log_event))
}

Benchee.run(
  %{
    "take_pending_ids/2 (current, end-to-end)" => fn events ->
      IngestEventQueue.take_pending_ids(key, length(events))
    end,
    "inline: select + per-id select_replace (CAS)" => fn events ->
      take_via_select_replace.(tid, length(events))
    end,
    "inline: select + uniq_by + update_element" => fn events ->
      take_via_update_element.(tid, length(events))
    end,
    "inline: select + per-id update_counter (CAS-lite)" => fn events ->
      take_via_update_counter.(tid, length(events))
    end,
    "inline: select + single compiled select_replace" => fn events ->
      take_via_batched_select_replace.(tid, length(events))
    end
  },
  inputs: inputs,
  before_each: fn events ->
    # Reset the queue to a fully-pending state before each claim.
    IngestEventQueue.truncate_table(key, :all, 0)
    IngestEventQueue.add_to_table(key, events)
    events
  end,
  time: 5,
  warmup: 2,
  memory_time: 3,
  reduction_time: 3
)

# credo:disable-for-this-file Credo.Check.Readability.MaxLineLength
# Run with: mix run test/profiling/ingest_event_queue_take_pending_ids_bench.exs
#
# Compares the claim strategy in take_pending_ids/2. The module now uses update_counter
# (the "current, end-to-end" line), so the inline strategies below are the alternatives:
#   - update_counter (current): per-id atomic CAS on the claim counter; no match-spec compile.
#   - select_replace (prior, #3609): per-id conditional CAS, compiles a match spec per id.
#   - update_element: dedup + unconditional write; correct only for single-consumer queues.
#   - single compiled select_replace: REJECTED (ETS scans an N-clause spec; see below).
#
# Historical results (indicative, local dev machine — capture formal snapshots against
# a committed SHA per the profiling workflow):
#
## 2026-06-19  Baseline: select_replace (current) vs update_element ##
# ##### With input 1000 events #####
# Name                                                   ips        average         99th %
# inline: select + uniq_by + update_element           5.13 K      194.86 μs      265.96 μs
# take_pending_ids/2 (current, end-to-end)            1.64 K      608.62 μs      670.72 μs
# inline: select + per-id select_replace (CAS)        1.63 K      613.49 μs      737.88 μs
# -> update_element ~3.15x faster (+413 μs/claim saved vs current)
#
# ##### With input 250 events #####
# Name                                                   ips        average         99th %
# inline: select + uniq_by + update_element          19.50 K       51.29 μs       68.45 μs
# take_pending_ids/2 (current, end-to-end)            6.58 K      151.96 μs      185.61 μs
# inline: select + per-id select_replace (CAS)        6.47 K      154.61 μs      183.41 μs
# -> update_element ~3.0x faster
#
# Note: update_element shows higher BEAM memory/reductions (the uniq_by set), but lower
# wall-clock — select_replace's per-id match-spec compilation happens in the BIF layer
# and does not surface as reductions. update_element is only safe for single-consumer
# queues (see the concurrent-claim regression test in ingest_event_queue_test.exs).
#
## 2026-06-19  Add update_counter (CAS-lite) variant ##
# ##### With input 1000 events #####
# Name                                                        ips      average       99th %    memory
# inline: select + per-id update_counter (CAS-lite)        7.38 K    135.57 μs    175.71 μs    109 KB
# inline: select + uniq_by + update_element                5.27 K    189.59 μs    222.08 μs    378 KB
# take_pending_ids/2 (current, end-to-end)                 1.67 K    599.56 μs    725.01 μs    266 KB
# inline: select + per-id select_replace (CAS)             1.51 K    660.62 μs    824.71 μs    266 KB
# -> update_counter ~4.4x faster than current AND multi-consumer safe (the 0->1 winner
#    claims; 2+ means another consumer/resize-dup already took it, so no uniq_by needed,
#    hence the lowest memory/reductions too). Requires a 5th integer "claim" field on the
#    row tuple ({id, status, event, size, claim}), which touches the module's match specs.
#
## 2026-06-19  Single compiled select_replace variant — REJECTED ##
# ##### With input 1000 events #####
# Name                                                        ips      average       99th %    memory
# inline: select + per-id update_counter (CAS-lite)        7.21 K    138.63 μs    166.44 μs    109 KB
# inline: select + per-id select_replace (CAS)             1.61 K    620.37 μs    712.39 μs    266 KB
# inline: select + single compiled select_replace          0.27 K   3678.24 μs   6588.41 μs    537 KB
# -> "compile once" is a trap: one match spec with N literal-key clauses is ~26x slower than
#    update_counter and ~6x slower than per-id select_replace. ETS does NOT index a
#    multi-clause literal-key spec into N keyed lookups — it SCANS, testing each row against
#    the clause list (~O(rows * clauses)), and compiling the giant spec is itself costly.
#    The per-id structure is what preserves the set-table key index; you cannot have both
#    "one compilation" and "keyed lookups" with select_replace. Do not revisit this approach.
#
## 2026-06-19  update_counter shipped in take_pending_ids/2 (5-tuple row) ##
# ##### With input 1000 events #####
# Name                                                        ips      average       99th %
# inline: select + per-id update_counter (CAS-lite)        7.87 K    127.04 μs    165.20 μs
# take_pending_ids/2 (current, end-to-end)                 7.57 K    132.08 μs    280.76 μs
# inline: select + uniq_by + update_element                5.24 K    190.97 μs    214.75 μs
# inline: select + per-id select_replace (CAS)             1.53 K    651.72 μs    714.56 μs
# inline: select + single compiled select_replace          0.28 K   3519.09 μs   3880.80 μs
# -> The real end-to-end function (now update_counter) tracks the inline CAS-lite line and
#    is ~4.9x faster than the prior per-id select_replace, while keeping multi-consumer
#    safety. Row tuple is now {event_id, status, event, size, claim}.
