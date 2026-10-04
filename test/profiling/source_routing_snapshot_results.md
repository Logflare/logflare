# Routing snapshot benchmark and lifetime protocol

## Comparison

Current parent: `130bdeb6` (#3942, the rollback of #3937). The implementation was
rebased onto that revision before the final comparison below. An earlier,
separately labeled comparison against `1bff0f28` (the #3937 regression baseline)
is retained for context. Each comparison uses two runs per revision in the same
workspace/runtime, with an identical harness within that comparison.

Environment: Linux, 6 available cores, Elixir 1.19.5, OTP 27.3.4.6, JIT enabled.
Benchee 1.5.0: parallel 1, warmup 2 seconds, measurement 5 seconds, memory 2 seconds
per scenario. The sequential router is the reference; `pre_check: :all_same`
passes, and fixture match counts are independently asserted before measurement.
These are local microbenchmarks, not production throughput or CI-parity results.

```sh
MIX_ENV=test ../bin/x mix run test/profiling/source_routing_bench.exs
MIX_ENV=test ROUTING_BENCH_STAGES=1 ../bin/x mix run test/profiling/source_routing_bench.exs
```

For the parent, copy the candidate harness outside the checkout, switch to the
parent revision using jj, and run that same file via `mix run`. No dependencies
or build caches are copied between workspaces.

### End-to-end latency against current main (#3942)

Each cell contains the average from run 1 / run 2. Speedup is the ratio of the
two-run arithmetic means, using the rounded Benchee output.

| Rules | Matches | Current main | Rebased candidate | Speedup |
| --- | ---: | ---: | ---: | ---: |
| 100 | 8 | 13.35 / 13.58 us | 7.63 / 7.20 us | 1.82x |
| 100 | 1 | 11.86 / 12.08 us | 11.28 / 10.34 us | 1.11x |
| 100 | 100 | 152.26 / 161.65 us | 74.60 / 72.57 us | 2.13x |
| 1,000 | 8 | 47.52 / 48.30 us | 39.04 / 38.97 us | 1.23x |
| 1,000 | 1 | 88.71 / 89.48 us | 86.07 / 86.89 us | 1.03x |
| 1,000 | 1,000 | 1.52 / 1.54 ms | 0.88 / 0.90 ms | 1.72x |

The single-match results are effectively comparable, especially at 1,000 rules;
do not interpret a 3% difference as a demonstrated throughput improvement.
The main benefits over the rollback are same-query snapshot consistency and
dense routing. The current-main `get_rules/1` API and its tests remain intact,
but snapshot routing no longer depends on individual cache entries.

Both revisions bulk-prefill individual rule cache entries from the fixture's
list query before measurement. This represents a fully warmed cache without
thousands of redundant setup queries. Two earlier candidate attempts with
per-rule database warmup timed out before producing summaries; neither is used
in these results.

### Earlier comparison against the #3937 regression baseline

This comparison predates #3942/rebase and used the same corrected match fixtures,
but did not prefill individual rule cache entries (neither implementation used
them). It is **not** the current-main speedup claim.

| Rules | Matches | #3937 baseline | Pre-rebase candidate | Speedup |
| --- | ---: | ---: | ---: | ---: |
| 100 | 8 | 46.29 / 46.52 us | 7.41 / 6.75 us | 6.55x |
| 100 | 1 | 66.79 / 63.53 us | 10.96 / 10.25 us | 6.14x |
| 100 | 100 | 99.33 / 99.25 us | 72.52 / 72.11 us | 1.37x |
| 1,000 | 8 | 443.34 / 430.43 us | 40.53 / 39.19 us | 10.96x |
| 1,000 | 1 | 603.11 / 592.24 us | 87.35 / 86.92 us | 6.86x |
| 1,000 | 1,000 | 1.03 / 1.01 ms | 0.90 / 0.90 ms | 1.13x |

The small sparse cases have high relative variance (candidate 100/8 deviation
91-130%). Their medians are 6.09 / 5.97 us versus the parent's 43.35 / 43.09 us.
The 1,000/8 medians are 36.79 / 36.40 us versus 390.66 / 382.46 us. The sparse
improvement is substantially larger than that noise. Dense results improve in
both final runs, rather than restoring the old per-rule-cache dense cost.

**Fixture correction:** the previous `metadata.rule_id:rule-100` literal was
unquoted. The parser interpreted it as equality to `"rule"` plus a negated
message term, so the scenarios called "one matching" actually matched zero
rules. This PR quotes the literal and checks that the scenarios really match
8, 1, or all rules. Earlier v1.50.9/main "one match" numbers are not comparable
to these corrected measurements. v1.50.9 was not rerun for this PR.

### Isolating the copy cost

Stage measurements have prefetched headers/trees and intentionally different
return values; their pre-check is disabled. They are diagnostics, not timings
to add together as if they had identical allocation/GC behavior.

For 1,000 rules / 8 matches:

| Stage | Parent average | Candidate average |
| --- | ---: | ---: |
| Cache fetch | 433.34 us | 30.40 us |
| Matching IDs with resident tree | 6.17 us | 5.81 us |
| Resolving IDs with resident header | 0.119 us | 2.19 us |

This isolates full-map cache retrieval as the dominant sparse-path cost.
Tree copying still scales with tree size; this change removes **unmatched rule
payload** copying, not all O(number of rules) work from every possible tree.

### Memory and cold construction

A separate probe modeled **current main's fully warmed routing cache** with real
Cachex entries: one tree header and one `get_rule` entry per rule. The candidate
used a real Cachex header plus its store and two lifecycle indexes. Common list
cache entries and fixed empty-table overhead are excluded. The figures count
ETS memory deltas plus candidate index/fallback binary bytes, not VM RSS.

| Rules / matches | Current-main layout bytes | Candidate layout bytes |
| --- | ---: | ---: |
| 100 / 8 | 153,016 | 149,273 |
| 100 / 1 | 185,064 | 181,746 |
| 100 / all | 178,144 | 173,569 |
| 1,000 / 8 | 1,546,696 | 1,485,307 |
| 1,000 / 1 | 1,864,632 | 1,807,773 |
| 1,000 / all | 1,795,024 | 1,720,419 |

The modeled candidate layout is about 2-4% smaller than the rollback's warmed
routing layout. This is not a claim about total production cache memory, where
other consumers can retain list or individual-rule entries too.

The following older allocation/footprint measurements compare against **#3937**,
not the rollback. This is not a blanket allocation reduction. Benchee's reported per-operation
memory for 1,000/8 increases from 3.62 KB to 102.95 KB, while 1,000/all decreases
from 785.76-787.27 KB to 671.97-674.10 KB. These are process allocation statistics,
not total retained cache memory or RSS.

A separate fixture footprint probe counted ETS memory deltas (including the
candidate's source/expiry indexes and header) plus its external index/fallback
binary byte sizes. It excludes fixed empty-table overhead and is not a whole-VM
RSS measurement; common external binary payloads are not separately counted.

| Rules / matches | Parent ETS bytes | Candidate ETS + snapshot binary bytes |
| --- | ---: | ---: |
| 100 / 8 | 141,512 | 149,398 |
| 100 / 1 | 173,528 | 182,006 |
| 100 / all | 166,528 | 173,555 |
| 1,000 / 8 | 1,414,200 | 1,485,227 |
| 1,000 / 1 | 1,731,848 | 1,808,882 |
| 1,000 / all | 1,662,144 | 1,725,046 |

The measured footprint increase is about 4-6%. Compression level 1 keeps the
backup small without adding codec work to normal reads. One-shot snapshot
construction (sorting, compression and store publication; excludes DB/tree
construction) took 0.38-0.48 ms for 100 rules and 3.75-3.95 ms for 1,000 rules.
These construction measurements were not repeated latency distributions.

A six-reader Benchee attempt was killed with exit -9 before producing results.
The cause was not established; no concurrent-throughput improvement is claimed.
Concurrency correctness is covered separately by the test suite.

## Lifetime protocol

1. One `Rules.rules_tree_by_source_id/1` query produces the tree and complete map.
2. Sort rule IDs into a compact binary index. Store a tuple keyed by
   `{source_id, make_ref()}`, with `{id, rule}` entries at known tuple positions.
3. Publish the complete `{tree, snapshot_header}` as one Cachex value only after
   compression and ETS insertion succeed. Headers carry the ETS table identity,
   generation, rule count, index binary and compressed map binary.
4. Sparse reads binary-search IDs and use `:ets.lookup_element/3`. Dense reads
   (at least half the rules) copy the tuple once and reconstruct the map. Neither
   path calls a GenServer, reads individual rules from the database, or resolves
   against a different generation.
5. If **any** required ETS lookup loses its generation/table, discard the partial
   result and resolve all IDs from that header's compressed map. Even a reader
   suspended across eviction, rebuild or store restart retains its exact data.
6. The store keeps only one generation per source, at most 100,000 sources, with
   a one-hour TTL and five-minute sweep. Publication also prunes expired/overflow
   entries. Data, source and expiry indexes retire together; old cache busts are
   generation-qualified and cannot retire a newer publication.

The compressed binary is the VM-managed reader reference. No explicit reader
lease/refcount server or fixed grace-period safety assumption is necessary.
Crashing readers release their binaries through the VM. Suspended readers can
retain their acquired data, but never pin a growing store-side list of retired
generations.

Cachex remains authoritative for header expiry/limit/clear and WAL-driven
`Rules.Cache.bust_by/1`. Source busts also request asynchronous retirement of the
exact backing generation. Cachex expiry/clear/limit can leave disposable backing
data until replacement/store TTL/capacity cleanup; this cannot cause partial
snapshots. The list warmer is unchanged and cannot publish partial routing
snapshots. Snapshot data remains per-node.

## Tradeoffs / draft review points

- The 50% dense threshold is a conservative hybrid policy, not a proven optimum
  across all rule shapes; intermediate match densities need production-shaped
  measurements before tuning it.
- Invalidated/evicted generations use slower full-map decompression. After a
  store restart, already cached headers keep using their fallback until header
  invalidation/expiry; this implementation does not backfill those headers.
- Cold publication is serialized in one store process. Hot reads are not.
  Large simultaneous cold rebuilds and large-source capacity deserve load tests.
- The store is bounded by source count, not an absolute byte budget. Memory held
  by active readers is managed by normal VM reference counting/GC.
- No production rollout, hosted throughput claim, or full-suite run is included.

## Validation and artifacts

104 tests, 0 failures, 1 existing excluded benchmark test across:

- `test/logflare/rules/routing_snapshot_test.exs`
- `test/logflare/rules/rule_cache_test.exs`
- `test/logflare/sources/source_router/rules_tree_test.exs`
- `test/logflare/sources/source_router_test.exs`
- `test/logflare/context_cache_test.exs`
- `test/logflare/context_cache/gossip_test.exs`
- `test/logflare/context_cache/cache_buster_test.exs`
- `test/logflare/backends/spool/consumer_pipeline_test.exs`

Coverage includes sparse/dense/fallback equivalence, missing/nil IDs, bigint
index boundaries, fixed header heap size, concurrent rebuilds, paused/crashed
readers, capacity/expiry, late retirement, store restart, cache bust/expire/clear,
and existing nil-rule/spool error-isolation regressions.

Local captured artifacts (not portable repository links):

- `/tmp/agent-capture-routing-main-final{1,2}.log` (current-main comparison)
- `/tmp/agent-capture-routing-rebased-final{1,2}.log` (current candidate)
- `/tmp/agent-capture-routing-current-main-footprint.log`
- `/tmp/agent-capture-routing-final-validation.log`
- `/tmp/agent-capture-routing-parent-release{1,2}.log`
- `/tmp/agent-capture-routing-candidate-release{1,2}.log`
- `/tmp/agent-capture-routing-parent-stages-corrected.log`
- `/tmp/agent-capture-routing-candidate-stages-release.log`
- `/tmp/agent-capture-routing-snapshot-footprint-compressed.log`
- `/tmp/agent-capture-routing-snapshot-final-tests.log`

Rejected prototypes: full binary decode regressed dense matching roughly 3x;
selecting fields out of a single ETS map did not remove sparse scaling. Indexed
ETS tuple elements avoid both costs on the normal path.
