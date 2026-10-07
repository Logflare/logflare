# Store-only routing restore: guarantees and tradeoffs

This report supersedes the transactional repair, publisher monitoring, header
retirement and memory-bound claims in the earlier routing hardening reports.
Their hot-path and publication measurements remain historical measurements of
those revisions, not measurements of this implementation.

[Steady-state and cold-publication follow-up](source_routing_store_only_steady_results.md)
now measures the published store-only boundaries against their predecessors,
including small hot-path regressions and all noisy confirmation runs.

## Implementation

- The production ETS table is named `Logflare.Rules.RoutingSnapshotStore`.
  Snapshot headers retain that stable name, not a restart-sensitive table ID.
  Isolated tests may use anonymous tables. Generation keys use monotonic positive
  integers generated in the reader's VM; named-table restart cannot mix targets.
- Restore sends the original key and compressed backup to the store. It never
  replaces a Cachex header, extends its TTL, or creates a new snapshot generation.
  The caller routes from its exact decoded fallback and keeps it only in the
  prepared batch. Normal successful ETS reads retain their existing paths.
- A newer or equal resident generation wins before decoding. Duplicate restores
  do not renew store TTL. Restore refuses source-count/estimated-byte overflow
  without evicting another source. Cold publication retains the existing policy
  of allowing one individually oversized snapshot.
- A public admission table contains at most 256 hash-slot claims. Same-source
  concurrent requests coalesce; slot collisions drop requests harmlessly. Only
  compressed backups cross the mailbox; decoding and parent map-to-tuple sorting
  occur in the store after generation/capacity checks. Periodic pruning clears
  claims abandoned between reservation and send. Admission tokens and table IDs
  prevent pre-prune/pre-restart requests from taking over newer claims.
- Cachex repair and conditional retirement transactions are removed, including
  startup transaction enablement, retirement tasks and publisher monitoring.
  Source-aware/ID-only invalidation and generation-qualified store deletion remain.
  Expiration, capacity eviction and publisher death leave headers alone.

## Safety boundaries

An old reader may repopulate an otherwise empty store after invalidation or
restart. That is an unreachable or old-reader-only acceleration row: no header is
resurrected, reads check the exact generation, and a newer resident generation
cannot be replaced. Rows expire or are replaced/evicted under store limits. No
unbounded generation tombstone map or reader registration is introduced.

The change does **not** make cold DB-load publication atomic with invalidation.
Monotonic generation ordering describes snapshot creation, not database freshness.

The store budget bounds conservative weights of **resident acceleration only**.
Its historical weight still includes an allowance for the tree/index/backup, but
removing retirement means that allowance no longer bounds corresponding objects
retained in Cachex. Cachex headers remain independently bounded by its 100,000-entry
limit and 60-minute TTL. There is no cross-cache byte/RSS guarantee. Reader-owned
snapshots, decoded batch state, fixed admission bookkeeping and queued compressed
payloads are outside the resident budget. Admission bounds normal concurrent
restore requests by count, not by bytes.

## Synthetic concurrent-miss measurements

Identical branch-neutral harness: `source_routing_restore_bench.exs`.
Baselines: published parent `acc8f99f`, child `229bb1d0`.
Candidates: local parent changes `yvmqpnoz`/`qrvomquq`, additive positional merge
`okqymlxy`. Complete raw observations: `source_routing_restore_results.json`.

Linux aarch64, OTP 27.3.4.6/JIT, Elixir 1.19.5, six schedulers. Five repetitions
per case, no Benchee warmup. Each case removes 32 published source rows, releases
32 readers together, and resolves 100 events per reader over 1,000 targets.
Sparse matches select eight targets; dense matches select all 1,000. Every event
asserts the complete expected targets. The driver mirrors the production fallback
state transition but excludes tree matching, DB, fixture construction and sink I/O.

Reader timing ends when all readers finish. Combined timing additionally drains
the asynchronous store with a synchronous barrier. Values are medians in ms;
median drain time is not necessarily the difference between these two medians.

| Boundary / matches | Published readers | Store-only readers | Published combined | Store-only combined |
| --- | ---: | ---: | ---: | ---: |
| Parent / eight | 7.251 | 1.230 | 7.253 | 7.728 |
| Parent / all | 58.223 | 12.941 | 58.231 | 13.301 |
| Positional child / eight | 2.997 | 0.610 | 2.999 | 2.307 |
| Positional child / all | 13.086 | 8.385 | 13.092 | 8.390 |

This demonstrates decoupled reader latency, **not a universal throughput gain**:
parent sparse combined time is about 6.5% higher. Dense parent improvement also
includes avoiding repeated dense-map reconstruction after fallback within a batch.
Hash-slot collisions leave 30–32 resident sources on the candidates, versus 32
on the baselines; all readers remain correct and all queued requests drain.

## Retained-memory tradeoff

The same harness publishes 32 sources with 1,000-target snapshots and a synthetic
1,000-entry equality-index tree per header into a store limited to eight sources.
It drains baseline retirement before sampling, then resolves each acquired
snapshot once. All five repeats give byte-identical footprints before/after routing.

| Boundary | Published headers | Store-only headers | Published ETS delta | Store-only ETS delta | Published header binaries | Store-only header binaries |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Parent | 8 | 32 | 757,440 B | 1,483,136 B | 131,661 B | 526,789 B |
| Positional child | 8 | 32 | 564,800 B | 1,288,576 B | 32,015 B | 128,043 B |

Resident source count stays eight; conservative store weights stay 379,581 B
(parent) and 230,031 B (child). The additional retained headers/trees are a real
cost of dropping retirement. ETS deltas cover Cachex and store-owned tables, not
VM RSS. Header binary counts sum logical encoded/index byte sizes separately;
ETS table-memory statistics do not account for all off-heap binary payloads.
The driver deliberately holds acquired snapshots for correctness checks, so these
numbers are not process-heap/RSS comparisons or guarantees about production churn.

## Validation

- Parent: seven complete focused files, 153 tests, zero failures, one existing exclusion.
- Child: same complete files, 154 tests, zero failures, one existing exclusion.
- Both: seven backend/source/rules/context-cache/spool consumer files, 195 tests,
  zero failures, four existing exclusions.
- Deterministic tests cover nonblocking restore/invalidation with a suspended
  store, stable-generation restart, late/duplicate restore, capacity rejection,
  bounded/coalesced admission, abandoned claims, TTL/accounting, publisher death,
  delayed publication and unchanged Cachex headers/TTL.
- Local `mix ci` gates and Dialyzer run separately on both boundaries. No gate,
  baseline or warning-suppression budget was relaxed.
- No full suite, coverage run, hosted CI, remote publication or production rollout.

Reproduce from either boundary or baseline with the same harness:

```sh
MIX_ENV=test ERL_FLAGS='+S 6:6' \
  ROUTING_RESTORE_REVISION=REVISION \
  ROUTING_RESTORE_OUTPUT=/tmp/routing-restore.json \
  ../bin/x mix run test/profiling/source_routing_restore_bench.exs
```

For historical revisions, save the harness outside the working copy and pass
that path to `mix run`. Each invocation runs in an isolated local VM and replaces
its test snapshot-store process with a controlled limit; never run it on a live node.
