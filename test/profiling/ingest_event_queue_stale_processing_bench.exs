alias Logflare.Backends.IngestEventQueue

# Compares stale-:processing detection strategies for QueueJanitor:
#
#   OLD (snapshot): select every :processing id, build a MapSet, intersect with the previous
#   cycle's MapSet, materialize the overlap. Allocates a list + two MapSets + intersection
#   every cycle, proportional to the number of in-flight rows, even when nothing is stale.
#   (Reconstructed here — this logic was removed from QueueJanitor by the timestamp change.)
#
#   NEW (timestamp): a single bounded :ets.select with a `claimed_at <= cutoff` guard. Returns
#   only the stale ids (usually none), allocating just that (typically empty) list.
#
# Both strategies read the same populated table and mutate nothing, so the table is shared.

user = Logflare.Factory.insert(:user)
source = Logflare.Factory.insert(:source, user: user)

owner = spawn(fn -> :timer.sleep(:infinity) end)
key = {source.id, nil, owner}
IngestEventQueue.upsert_tid(key)
tid = IngestEventQueue.get_tid(key)

# A single template event is reused for every row; only the id (the set key) must be unique,
# and detection inspects only status/claimed_at, never the event body.
template = Logflare.Factory.build(:log_event)
size = :erlang.external_size(template.body)

# Row tuple is 6 elements: {id, status, event, size, claim, claimed_at}.
build_rows = fn ids, status, claim, claimed_at ->
  for id <- ids, do: {id, status, template, size, claim, claimed_at}
end

# Populate `tid` for a scenario and return what each strategy needs:
#   * cutoff      — now - 30s (the janitor's staleness boundary)
#   * prev_ids    — the OLD strategy's previous-cycle MapSet: the still-stuck rows plus a
#                   disjoint set of decoys standing in for last cycle's in-flight rows that
#                   have since been acked (so the healthy-case intersection is empty, and the
#                   stale-case intersection is exactly the stuck rows — same answer as NEW).
populate = fn %{pending: pending, fresh: fresh, stale: stale} ->
  now = System.monotonic_time(:millisecond)
  cutoff = now - 30_000

  pending_ids = for i <- 1..pending//1, do: "pend-#{i}"
  fresh_ids = for i <- 1..fresh//1, do: "fresh-#{i}"
  stale_ids = for i <- 1..stale//1, do: "stale-#{i}"

  rows =
    build_rows.(pending_ids, :pending, 0, 0) ++
      build_rows.(fresh_ids, :processing, 1, now) ++
      build_rows.(stale_ids, :processing, 1, now - 60_000)

  :ets.delete_all_objects(tid)
  :ets.insert(tid, rows)

  decoys = for i <- 1..fresh//1, do: "prev-#{i}"
  prev_ids = MapSet.new(stale_ids ++ decoys)

  %{key: key, cutoff: cutoff, prev_ids: prev_ids}
end

old_detect = fn %{key: key, prev_ids: prev_ids} ->
  current = MapSet.new(IngestEventQueue.list_processing_ids(key))
  MapSet.intersection(current, prev_ids) |> MapSet.to_list()
end

new_detect = fn %{key: key, cutoff: cutoff} ->
  IngestEventQueue.list_stale_processing_ids(key, cutoff, 10_000)
end

inputs = %{
  "1k processing, none stale" => %{pending: 0, fresh: 1_000, stale: 0},
  "10k processing, none stale" => %{pending: 0, fresh: 10_000, stale: 0},
  "10k pending + 1k processing, none stale" => %{pending: 10_000, fresh: 1_000, stale: 0},
  "9k processing + 1k stale" => %{pending: 0, fresh: 9_000, stale: 1_000}
}

Benchee.run(
  %{
    "old: snapshot + MapSet intersection" => fn scenario -> old_detect.(scenario) end,
    "new: timestamp select" => fn scenario -> new_detect.(scenario) end
  },
  inputs: inputs,
  before_scenario: fn spec -> populate.(spec) end,
  time: 5,
  warmup: 2,
  memory_time: 3,
  reduction_time: 3
)

# credo:disable-for-this-file Credo.Check.Readability.MaxLineLength
# Run with: mix run test/profiling/ingest_event_queue_stale_processing_bench.exs
#
# Capture formal snapshots against an already-committed SHA per the profiling workflow.
#
## 2026-06-22  @ c812fc913  (old snapshot detection vs new timestamp select) ##
# Headline: the new select allocates ~none in the healthy (nothing-stale) case, where the
# old path built a MapSet of every in-flight row + an intersection every cycle.
#
# ##### 1k processing, none stale #####
# Name                                     ips     average      memory    reductions
# new: timestamp select                 53.69 K    18.62 μs    0.172 KB        1.01 K
# old: snapshot + MapSet intersection    9.70 K   103.06 μs   76.27 KB        11.42 K
# -> new ~5.5x faster, ~444x less memory
#
# ##### 10k processing, none stale #####
# new: timestamp select                  5.26 K    0.190 ms   0.82 KB        36.98 K
# old: snapshot + MapSet intersection    0.76 K     1.31 ms   1.18 MB       115.79 K
# -> new ~6.9x faster, ~1444x less memory
#
# ##### 10k pending + 1k processing, none stale #####
# new: timestamp select                  5.97 K   (baseline)  0.82 KB        40.99 K
# old: snapshot + MapSet intersection    3.87 K    +91.08 μs 113.95 KB        48.66 K
# -> both scan the full table for the 1k processing rows, so time is close (1.5x); the old
#    path still builds a MapSet of them, so memory is ~139x worse.
#
# ##### 9k processing + 1k stale #####
# new: timestamp select                  4.66 K     0.21 ms  0.0459 MB       36.98 K
# old: snapshot + MapSet intersection    0.68 K     1.47 ms    1.22 MB      116.52 K
# -> even when 1k rows are genuinely stale, new returns just those (small list) while old
#    still builds the full 10k MapSet: ~6.9x faster, ~27x less memory.
#
# Reads-only; both strategies return the same stale ids per scenario. The "scan still happens"
# caveat from the PR holds — time tracks table size — but the per-cycle allocation is gone.
