# Routing benchmark refresh: 10,000 rules per source

## Scope and measured revisions

- Current main: `52a4d2bcdc4df8ffa794431ac8487d0f5fee9f14`.
- Refreshed ID-keyed #3943: `787816c6cf3d5ae2189a96b4ed46ffd04f8225d3`.
- Refreshed positional #3946: `b01cd339a80c7262814b2cbb6643d0a3897d7c45`.

Current main was merged into #3943 and then propagated to #3946 with additive
merge commits. No published commits were rebased or rewritten. Subsequent report-only
commits do not change the measured production code or harness.

This report supersedes the earlier snapshot, hardening, and positional benchmark
notes for the current PR descriptions. Historical numbers used different fixtures
and revisions and are not comparable to this matrix.

## Method

Linux; six Erlang schedulers (`ERL_FLAGS='+S 6:6'`); Elixir 1.19.5;
OTP 27.3.4.6 with JIT; Benchee 1.5.0; parallel 1; warmup 1s;
time 3s; memory_time 1s. CPU model was unavailable to Benchee.

The exact same branch-neutral harness was run twice per revision, in order
main → ID-keyed → positional → positional → ID-keyed → main. Tables show the
arithmetic mean of the two per-run means. Per-run mean, median, deviation,
allocation, and footprint data are checked in as
[`source_routing_scale_results.json`](source_routing_scale_results.json).

These are **synthetic, fully cache-warm routing microbenchmarks**, not
DB-backed publication tests or end-to-end production-throughput measurements.
Fixtures use ordinary `%Rule{}` records with deterministic, non-contiguous IDs
and real LQL parsing/tree construction. Source sizes are 100, 1,000, and 10,000
rules. Shapes are quoted rule-ID equality plus a common severity predicate
(zero/one match), severity thresholds (eight matches), and identical metadata/type
plus severity predicates (all matches). Exact matching target IDs, backend IDs,
and sinks are asserted before measurement. Rule setup, parsing, tree construction,
snapshot publication, cleanup, and correctness checks are outside timed regions.

The event body contains only the fields needed for matching. Main fetches its
cached tree and per-rule records per event; the candidates prepare once per batch
and thread `matching_rules_with_state/3`, as the production router does. No sink
I/O is timed. Each operation returns all matched targets, preserving comparable
result-list allocation. Dense cases are capped at 100,000 returned targets per
operation: 10,000 rules × 100 events is deliberately omitted, not treated as a pass.

The replacement `source_routing_bench.exs` and `source_routing_batch_bench.exs`
are entry points to this shared harness; old sequential-reference and stage-mode
results remain historical rather than mixing them with these measurements.

## Single-event routing

| Rules / matches | Main | ID-keyed #3943 | Positional #3946 |
| --- | ---: | ---: | ---: |
| 100 / zero | 10.00 us | 10.57 us | 9.81 us |
| 100 / one | 11.69 us | 10.77 us | 10.02 us |
| 100 / eight | 13.12 us | 5.94 us | 4.92 us |
| 100 / all | 134.72 us | 18.59 us | 15.06 us |
| 1000 / zero | 105.70 us | 103.57 us | 105.82 us |
| 1000 / one | 101.55 us | 104.85 us | 105.60 us |
| 1000 / eight | 49.62 us | 39.41 us | 34.84 us |
| 1000 / all | 1.42 ms | 256.37 us | 179.03 us |
| 10000 / zero | 1.51 ms | 3.74 ms | 1.75 ms |
| 10000 / one | 1.51 ms | 3.36 ms | 1.77 ms |
| 10000 / eight | 353.33 us | 370.02 us | 329.19 us |
| 10000 / all | 24.61 ms | 6.08 ms | 3.02 ms |

## 10,000-rule batch routing

| Matches / events | Main | ID-keyed #3943 | Positional #3946 |
| --- | ---: | ---: | ---: |
| zero / 10 | 15.30 ms | 15.25 ms | 10.59 ms |
| zero / 100 | 163.37 ms | 109.41 ms | 92.07 ms |
| one / 10 | 15.37 ms | 15.69 ms | 10.60 ms |
| one / 100 | 151.97 ms | 110.67 ms | 96.28 ms |
| eight / 10 | 3.39 ms | 764.10 us | 700.05 us |
| eight / 100 | 35.06 ms | 4.64 ms | 4.47 ms |
| all / 10 | 360.20 ms | 38.35 ms | 29.03 ms |

## 10,000-rule per-operation allocation

| Matches / events | Main | ID-keyed #3943 | Positional #3946 |
| --- | ---: | ---: | ---: |
| zero / 1 | 3.92 MiB | 3.92 MiB | 3.92 MiB |
| zero / 10 | 39.21 MiB | 40.36 MiB | 40.36 MiB |
| zero / 100 | 393.69 MiB | 404.24 MiB | 402.28 MiB |
| one / 1 | 3.92 MiB | 3.92 MiB | 3.92 MiB |
| one / 10 | 39.22 MiB | 40.38 MiB | 40.36 MiB |
| one / 100 | 393.75 MiB | 403.20 MiB | 406.27 MiB |
| eight / 1 | 877.87 KiB | 867.57 KiB | 863.36 KiB |
| eight / 10 | 6.05 MiB | 927.34 KiB | 885.93 KiB |
| eight / 100 | 64.74 MiB | 1.49 MiB | 1.09 MiB |
| all / 1 | 29.59 MiB | 10.36 MiB | 10.45 MiB |
| all / 10 | 303.14 MiB | 92.31 MiB | 92.35 MiB |

## Retained routing-cache memory at 10,000 rules

ETS memory deltas include the named Rules.Cache table and all data/source/expiry
tables owned by RoutingSnapshotStore. Measurements subtract empty-table memory,
excluding other application tables, VM heap/RSS, and allocator fragmentation.
They reproduced byte-for-byte across both runs.

“Representative” primes only main's records that match the representative event;
“fully warm” primes all main records. Candidates publish all compact targets in
either state. This exposes the sparse first-event floor rather than comparing
only the most favorable fully warmed baseline.

| Shape | Main representative | Main fully warm | ID-keyed (either) | Positional (either) |
| --- | ---: | ---: | ---: | ---: |
| zero | 1.20 MiB | 17.67 MiB | 1.81 MiB | 1.58 MiB |
| one | 1.20 MiB | 17.67 MiB | 1.81 MiB | 1.58 MiB |
| eight | 870.47 KiB | 14.19 MiB | 1.45 MiB | 1.22 MiB |
| all | 16.94 MiB | 16.94 MiB | 1.37 MiB | 1.15 MiB |

## Stale-generation fallback (10,000 rules / 100 events)

The snapshot is acquired before replacing its generation and cache header. The
old reader must use its exact immutable fallback and cannot repair over the newer
header. Production state threading carries the decoded fallback through the
remaining events. Replacement and invalidation are outside the timed operation;
first-event decoding and conditional repair checking are inside it. Two runs per
representation; not a restart/outage-throughput or concurrent-replacement claim.

| Matches | ID-keyed time / allocation | Positional time / allocation |
| --- | ---: | ---: |
| one | 100.20 ms / 407.87 MiB | 98.00 ms / 403.74 MiB |
| eight | 11.74 ms / 231.22 KiB | 10.87 ms / 231.48 KiB |

## Prepare-once check on the positional head

One additional adjacent run compares state-threaded batches with repeated public
per-event calls. This supports the batch-reuse mechanism; it is not a second
independent main-versus-final speedup claim.

| Matches / events | Speedup | Allocation reduction |
| --- | ---: | ---: |
| zero / 10 | 1.66x | 1.23x |
| zero / 100 | 1.55x | 1.34x |
| one / 10 | 1.66x | 1.23x |
| one / 100 | 1.88x | 1.33x |
| eight / 10 | 4.33x | 6.83x |
| eight / 100 | 7.22x | 58.32x |
| all / 10 | 1.07x | 1.04x |

## Interpretation and limitations

- At 10,000 rules / eight matches / 100 events, #3943 is **7.56x faster**
  than current main; the final stack is **7.84x faster** with
  **98.3% lower allocation**.
- Dense 10,000-rule single-event routing is **4.05x faster** on #3943 and
  **8.15x faster** on the final stack.
- **Single-event sparse regressions are not resolved.** At 10,000 rules, #3943 is
  **2.49x slower with zero matches** and
  **2.22x slower with one match**. The positional child narrows these to
  **1.16x** and **1.17x slower**, respectively. The one-match parent means were
  3.73 ms / 2.98 ms; these are substantial losses despite run-to-run variance.
- At 1,000 rules / one match / one event, the parent and child are near main
  (101.55 us / 104.85 us / 105.60 us). This does not erase the older fixture's reported regression;
  historical measurements are explicitly not comparable.
- Zero/one-match 10,000-rule 100-event batches improve elapsed time but do not
  reduce allocation: final is about 2–3% higher than main. Matching work remains
  costly in these shapes; the optimization is not a blanket allocation win.
- Fully warmed positional routing caches use about **91–93% less ETS memory**
  at 10,000 rules, but their representative sparse floor is **32–44% higher** than
  main. The ID-keyed floor is higher still. Removing IDs/index reduces retained
  candidate memory by approximately 13–17% at this scale.
- Routing targets, tree/header copying, matching, and list construction are
  included. Cold database queries/publication, source churn, eviction pressure,
  concurrent readers, sink work, total RSS, and rollout behavior were not measured.
- The 50% sparse/dense threshold is not proven optimal. These results warrant
  production-informed profiling of zero/one-match cases, not an unsupported
  universal-performance claim or a speculative rule-tree rewrite.

## Reproduction

Use the same file bytes for all three revisions (copy the shared harness outside
the workspace before selecting current main). Run serially, never simultaneously:

```sh
MIX_ENV=test ERL_FLAGS='+S 6:6' ROUTING_BENCH_OUTPUT=/tmp/routing-scale.json \
  ../bin/x mix run test/profiling/source_routing_scale_bench.exs

MIX_ENV=test ERL_FLAGS='+S 6:6' ROUTING_BENCH_RULES=10000 \
  ROUTING_BENCH_BATCHES=100 ROUTING_BENCH_FALLBACK=1 \
  ROUTING_BENCH_OUTPUT=/tmp/routing-fallback.json \
  ../bin/x mix run test/profiling/source_routing_scale_bench.exs

MIX_ENV=test ERL_FLAGS='+S 6:6' ROUTING_BENCH_RULES=10000 \
  ROUTING_BENCH_OUTPUT=/tmp/routing-batch-reuse.json \
  ../bin/x mix run test/profiling/source_routing_batch_bench.exs
```

## Refreshed validation

- ID-keyed: 94 focused routing/cache/snapshot tests, 0 failures, 1 excluded.
- Positional: 95 focused routing/cache/snapshot tests, 0 failures, 1 excluded.
- Each head: 79 affected rules/context-cache/BigQuery consumer tests plus one
  property, 0 failures, 2 excluded.
- Each head: full formatter check, test-environment compile, lint.all, and
  test.typings passed. Lint reported 31 existing design suggestions; typings
  reported 156 configured skips and 13 unnecessary skips.
- All six hot matrix runs and four stale-fallback runs completed successfully;
  single-reader only. Hosted CI is checked separately after publication.
