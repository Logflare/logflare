# Routing review hardening: correctness and performance

## Scope and revisions

This records the earlier transactional hardening of #3943 and #3946. [Store-only restore results](source_routing_restore_results.md) supersede its repair, retirement and memory-bound claims; the measurements below apply only to the recorded revisions. Earlier scale/snapshot reports remain historical. [Steady-state/cold follow-up](source_routing_store_only_steady_results.md) measures the subsequent store-only boundaries; it does not relabel the measurements below as current.

- Main: `52a4d2bc`.
- Published parent / child before fixes: `cc3b304b` / `b27aea47`.
- Corrected full-matrix boundaries: parent `5f88dfaf`, child `52040a8a`.
- Paired repeat boundaries after generic-primary-key compatibility: parent `686a501f`, child `cac3e7ca`.
- Subsequent report commits do not change runtime code. Generic-primary-key compatibility does not change matching or publication paths.

The parent owns startup transactions, source-aware and ID-only invalidation (including generic primary-key dispatch), monitored cold publication/retirement, exported/polled gauges, and synchronized footprint sampling. The child only adapts these to positional targets; no reader acquisition/release or per-event coordination was added.

## Method

Linux; Elixir 1.19.5; OTP 27.3.4.6/JIT; Benchee 1.5.0; `ERL_FLAGS='+S 6:6'`; parallel 1; warmup 1s/time 3s/memory 1s. All revisions use the identical branch-neutral harness. CPU model was unavailable.

Two full 35-case hot matrices per boundary cover 100/1,000/10,000 rules, zero/one/eight/all matches and 1/10/100 events. 10k x 100 all-matching events is omitted under the 100,000-returned-target cap. Two additional alternating before/after rounds check the initially noisy 100-rule parent and 10k-rule child cases. Aggregates use every available repeat for each case (run counts are in JSON). Initial sequence: corrected parent twice, published parent twice, published child twice, corrected child twice, main twice; targeted repeats alternate before/after.

Hot timing excludes fixture setup, parsing, DB, publication and sink I/O. It threads the production batch API on candidates; main uses per-event lookups. Exact target assertions run before measurement. These are synthetic microbenchmarks, not end-to-end throughput claims.

## Hot before/after guard

The matching path and snapshot payload are unchanged by these fixes. Initial full matrices had timing outliers: parent up to +6.7%, child up to +12.9%. Paired repeats did not reproduce those specific large outliers: parent all-match/10-event was -1.1%; child 10k timings range -3.9% to +3.9% (medians within about 4%). The paired parent zero-match single-event mean was +7.5%, but its median was +2.3%. This is not a universal zero-regression guarantee.

Full-matrix allocation differences are below 0.4% for the parent and 0.01% for the child, with most sparse measurements identical; some dense Benchee averages vary with GC. No added per-event allocation mechanism or per-rule reverse index was introduced.

## Corrected 10k routing vs main

| Matches / events | Main | ID-keyed parent | Positional child | Parent speedup | Child speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| zero / 1 | 1.49 ms | 3.19 ms | 1.76 ms | 0.47x | 0.85x |
| one / 1 | 1.50 ms | 2.73 ms | 1.80 ms | 0.55x | 0.83x |
| eight / 1 | 341.20 us | 380.91 us | 333.98 us | 0.90x | 1.02x |
| all / 1 | 22.78 ms | 6.14 ms | 3.18 ms | 3.71x | 7.16x |
| zero / 100 | 148.10 ms | 110.55 ms | 98.65 ms | 1.34x | 1.50x |
| one / 100 | 148.85 ms | 110.09 ms | 94.05 ms | 1.35x | 1.58x |
| eight / 100 | 35.22 ms | 4.65 ms | 4.63 ms | 7.58x | 7.60x |
| all / 10 | 345.99 ms | 37.50 ms | 31.05 ms | 9.23x | 11.14x |

A speedup below 1 means slower. Sparse single-event regressions remain: parent about 2.14x/1.82x slower (zero/one), child about 1.18x/1.20x slower. This work fixes correctness, not those pre-existing algorithmic regressions.

### 10k per-operation allocation

| Matches / events | Main | Parent | Child |
| --- | ---: | ---: | ---: |
| zero / 100 | 393.69 MiB | 404.24 MiB | 402.28 MiB |
| one / 100 | 393.75 MiB | 403.20 MiB | 406.27 MiB |
| eight / 100 | 64.74 MiB | 1.49 MiB | 1.09 MiB |
| all / 10 | 303.14 MiB | 92.30 MiB | 92.35 MiB |

At eight matches/100 events, parent is 7.58x faster with 97.7% less allocation; child is 7.60x faster with 98.3% less allocation. Zero/one-match 100-event batches still allocate about 2-3% more than main.

## Cold synthetic publication: explicit safety cost

One short-lived task performs invalidation/cleanup plus a store barrier, tree construction from already parsed rules, target encoding/registration and Cachex header publication. This exercises the publisher monitor/exit protocol but is not the production Courier/DB pipeline. Parsing, DB and sink I/O are excluded. Parent has two repeats; child has four, including paired repeats. **Only timing is interpreted: Benchee caller-memory values exclude worker allocations.**

| Rules | Parent before | Parent after | Change | Child before | Child after | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 100.12 us | 103.86 us | +3.7% | 92.77 us | 96.70 us | +4.2% |
| 1000 | 889.82 us | 906.36 us | +1.9% | 683.88 us | 703.27 us | +2.8% |
| 10000 | 10.81 ms | 10.82 ms | +0.0% | 8.07 ms | 8.52 ms | +5.6% |

Cold overhead is nonzero: about 0-4% parent and 3-6% child in aggregate; the paired 10k child subset was +6.7%. No zero-cost or production-throughput claim is made. Source-aware WAL invalidation avoids the legacy ID-only scan and primary-owner query. ID-only compatibility is deliberately control-plane work: it scans immutable cached headers and queries current ownership, including newly inserted/moved rules, without adding a hot-path reverse index.

## Synchronized retained ETS memory

Cleanup now drains store retirement before sampling. Both corrected footprint repeats are byte-identical within each boundary. These include Rules.Cache plus store data/source/expiry ETS tables, not VM RSS or transient publisher heaps. Main lazily caches matching Rule records; candidates publish all compact targets.

| 10k shape | Main representative | Main fully warmed | Parent | Child |
| --- | ---: | ---: | ---: | ---: |
| zero | 1.20 MiB | 17.67 MiB | 1.81 MiB | 1.58 MiB |
| one | 1.20 MiB | 17.67 MiB | 1.81 MiB | 1.58 MiB |
| eight | 870.47 KiB | 14.19 MiB | 1.45 MiB | 1.22 MiB |
| all | 16.94 MiB | 16.94 MiB | 1.37 MiB | 1.15 MiB |

Child fully warmed ETS is 91-93% lower, but its sparse representative floor is 32-44% higher than main; parent sparse floor is 51-71% higher. The new monitor field adds only a small per-source ETS metadata delta; steady-state target payloads are unchanged. Byte accounting remains an estimate and one oversized snapshot is allowed.

## Stale generation fallback

One 10k fallback run per corrected boundary covers one/eight matches and 1/10/100 events. Acquisition and replacement are outside timing; decoding and stale repair checks are inside. The old batch reuses its exact decoded generation. This is not an outage or concurrent-throughput benchmark.

| Matches / 100 events | Parent | Child | Parent allocation | Child allocation |
| --- | ---: | ---: | ---: | ---: |
| one | 105.65 ms | 94.03 ms | 407.87 MiB | 403.74 MiB |
| eight | 11.21 ms | 10.29 ms | 231.22 KiB | 231.48 KiB |

## Full corrected hot matrix

| Input | Main us | Parent us | Child us | Parent/main allocation | Child/main allocation |
| --- | ---: | ---: | ---: | ---: | ---: |
| 100 rules / all matches / 1 events | 132.49 | 19.28 | 14.50 | 0.2651x | 0.2408x |
| 100 rules / all matches / 10 events | 1350.81 | 164.08 | 117.85 | 0.1991x | 0.1838x |
| 100 rules / all matches / 100 events | 18942.82 | 1550.27 | 1163.45 | 0.2216x | 0.2079x |
| 100 rules / eight matches / 1 events | 13.12 | 6.20 | 5.32 | 0.5718x | 0.4886x |
| 100 rules / eight matches / 10 events | 136.76 | 20.75 | 13.72 | 0.2231x | 0.1404x |
| 100 rules / eight matches / 100 events | 1590.41 | 186.99 | 110.17 | 0.1759x | 0.0981x |
| 100 rules / one matches / 1 events | 10.76 | 9.93 | 11.05 | 0.9646x | 0.9241x |
| 100 rules / one matches / 10 events | 115.97 | 52.82 | 50.42 | 0.7049x | 0.7057x |
| 100 rules / one matches / 100 events | 1343.54 | 515.35 | 504.38 | 0.7167x | 0.7163x |
| 100 rules / zero matches / 1 events | 9.41 | 10.37 | 10.20 | 0.9809x | 0.9481x |
| 100 rules / zero matches / 10 events | 100.20 | 49.47 | 49.85 | 0.6589x | 0.6615x |
| 100 rules / zero matches / 100 events | 1078.05 | 500.72 | 489.60 | 0.7957x | 0.7976x |
| 1000 rules / all matches / 1 events | 1253.14 | 251.16 | 183.77 | 0.2280x | 0.2083x |
| 1000 rules / all matches / 10 events | 19276.02 | 2291.21 | 1638.57 | 0.2286x | 0.1938x |
| 1000 rules / all matches / 100 events | 270016.94 | 22591.25 | 16527.05 | 0.2775x | 0.2471x |
| 1000 rules / eight matches / 1 events | 46.74 | 38.94 | 35.58 | 0.3567x | 0.1905x |
| 1000 rules / eight matches / 10 events | 501.06 | 89.52 | 78.61 | 0.0807x | 0.0373x |
| 1000 rules / eight matches / 100 events | 4927.19 | 650.86 | 512.72 | 0.0692x | 0.0314x |
| 1000 rules / one matches / 1 events | 101.58 | 106.83 | 105.92 | 0.8907x | 0.8897x |
| 1000 rules / one matches / 10 events | 1001.91 | 661.86 | 655.98 | 0.8719x | 0.8717x |
| 1000 rules / one matches / 100 events | 9810.91 | 5352.86 | 5404.80 | 0.9936x | 0.9939x |
| 1000 rules / zero matches / 1 events | 94.85 | 104.41 | 106.60 | 0.9961x | 0.9960x |
| 1000 rules / zero matches / 10 events | 1063.57 | 640.70 | 666.38 | 0.8522x | 0.8535x |
| 1000 rules / zero matches / 100 events | 10755.48 | 5532.46 | 5634.10 | 0.9956x | 0.9956x |
| 10000 rules / all matches / 1 events | 22782.93 | 6137.28 | 3183.71 | 0.3502x | 0.3530x |
| 10000 rules / all matches / 10 events | 345994.07 | 37499.78 | 31048.16 | 0.3045x | 0.3046x |
| 10000 rules / eight matches / 1 events | 341.20 | 380.91 | 333.98 | 0.9883x | 0.9835x |
| 10000 rules / eight matches / 10 events | 3414.19 | 760.78 | 711.55 | 0.1496x | 0.1429x |
| 10000 rules / eight matches / 100 events | 35224.23 | 4649.49 | 4633.86 | 0.0230x | 0.0168x |
| 10000 rules / one matches / 1 events | 1497.43 | 2728.73 | 1795.45 | 0.9995x | 0.9992x |
| 10000 rules / one matches / 10 events | 14876.46 | 15343.03 | 10788.53 | 1.0295x | 1.0291x |
| 10000 rules / one matches / 100 events | 148848.57 | 110089.37 | 94051.37 | 1.0240x | 1.0318x |
| 10000 rules / zero matches / 1 events | 1491.87 | 3189.59 | 1764.19 | 0.9998x | 0.9997x |
| 10000 rules / zero matches / 10 events | 15032.43 | 15429.51 | 10820.31 | 1.0295x | 1.0293x |
| 10000 rules / zero matches / 100 events | 148095.12 | 110553.74 | 98648.90 | 1.0268x | 1.0218x |

## Validation

- Parent: 142 complete focused tests; child: 143; zero failures, one existing exclusion each.
- Parent: 200 complete backend/source/rules/WAL/cache/spool consumer tests; child: 187 plus 13 in a separate complete rules-file run; zero failures, four existing exclusions.
- `mix ci` passed on both: compile, formatting, lint, security scan, duplication and structure. Lint retains 31 existing suggestions; clone budget 26/26; 197 accepted structure findings suppressed.
- Dialyzer passed on both: 156 configured skips and 13 unnecessary skips. Security scan exits successfully but reports trusted local snapshot decoding; the new positional membership decoder uses `[:safe]`.
- Regression coverage includes pre-captured cache operations racing repair, real Courier publication evicted before commit, publisher death before/after commit, delayed cleanup vs replacement, subsequent retirement of completed publication, ID-only update/delete/insert/move, generic primary-key invalidation, WAL fast-path no-scan/no-query, and actual metric-exporter gauge/reset values.
- Hosted CI must be checked on the newly published heads; no local full suite or coverage run was performed.

## Reproduce

```sh
MIX_ENV=test ERL_FLAGS='+S 6:6' ../bin/x mix run test/profiling/source_routing_scale_bench.exs
MIX_ENV=test ERL_FLAGS='+S 6:6' ROUTING_BENCH_PUBLICATION=1 ../bin/x mix run test/profiling/source_routing_scale_bench.exs
MIX_ENV=test ERL_FLAGS='+S 6:6' ROUTING_BENCH_RULES=10000 ROUTING_BENCH_FALLBACK=1 ../bin/x mix run test/profiling/source_routing_scale_bench.exs
```

Set `ROUTING_BENCH_OUTPUT` for distinct per-run JSON/footprint artifacts. All raw runs, code boundaries, medians/deviations, caller-memory values and aggregate run counts are in [source_routing_review_hardening_results.json](source_routing_review_hardening_results.json). Production-shaped concurrency, source churn/eviction load, DB latency, sink I/O, total RSS and rollout remain unmeasured. Remaining sparse single-event profiling/algorithm tuning belongs in a separate PR.
