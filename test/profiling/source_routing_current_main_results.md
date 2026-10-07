# Routing snapshots: current-main performance and regression checks

## Revisions and evidence

- Pinned main: `6c1d54c13cb58c8a496b65588f02770e8f14f9d8`.
- Published parent #3943: `b32324541bad02984057a7287129c313876e33ca`.
- Published stacked child #3946: `1c89d80880a15677d9d46fc567394b5ac94dba48`.
- Later benchmark/documentation commits change no application runtime code.
- This is a whole-head comparison, **not** the PR patches applied onto main.
  Main includes unrelated commits after the PR branch base; especially the HTTP
  differences cannot be attributed exclusively to routing.
- [Raw runs and aggregates](source_routing_current_main_results.json) retain
  **126 valid VMs / 1,110 workload observations**, plus **72 superseded
  fixture-copy concurrency diagnostic observations**. No valid timing outliers
  were removed.

This supplements the [store-only lifecycle/memory report](source_routing_restore_results.md)
and [previous-head hot/cold comparisons](source_routing_store_only_steady_results.md).
Those comparisons remain correctly labeled historical or previous-head comparisons,
not substitutes for this pinned-main measurement.

## Protocol

Linux aarch64 agent environment, OTP 27.3.4.6/JIT, Elixir 1.19.5, Benchee 1.5.0,
Cachex 4.1.1, six schedulers (`ERL_FLAGS='+S 6:6'`), Benchee parallel 1.
Reported CPU model is unrecognized; host load was not continuously captured.

Identical branch-neutral timed paths in [scale driver](source_routing_scale_bench.exs),
[concurrency/retention driver](source_routing_main_bench.exs) and
[HTTP driver](source_routing_http_bench.exs), saved outside the workspace before
switching revisions. Routing targets are normalized and checked before timing.

Initial broad order: main/parent/child, then child/parent/main. Warmup/time/memory
1s/3s/1s. Longer confirmations: parent/main/child, main/child/parent, then
child/parent/main; warmup/time/memory 2s/5s/1s. Concurrency has no caller-memory
measurement because it does not capture reader-task allocations. Additional
eight-reader confirmations use main/parent/child then child/parent/main with
warmup 2s/time 8s. All benchmark VMs run serially, but the host is not dedicated.

Cases:

- Sparse: 100/1,000/10,000 rules; zero/one match; 1/2/5/10/100 events, five runs
  per head for every case.
- Dense sanity: 100/10,000 rules; eight/all matches; 1/10 events, two broad runs.
  The 10k/eight/singleton anomaly receives three additional runs.
- Empty/one-rule sources: zero/one event-body variants and 1/100 events; two broad
  runs, with three additional singleton runs. The zero-rule `one` label denotes
  the body variant, not an actual matching rule.
- Partial cache: 10k rules, zero/one matches, 1/10/100 events, five runs.
- Cold: 100/1,000/10,000 rules, eight-match fixtures, five runs.
- Concurrent: 10k rules, one match, 1/8/32 readers, 1/8 cached sources,
  1/10 events/reader, five runs; eight-reader cases have seven.
- Retention: six populations, five runs, three publication/read cycles each.
- HTTP: 100/10k rules, one matching sink, 1/10/100 events/request; five fresh VMs
  per head, 30 measured and five warmup requests per case.

Hot batches repeat a representative event. Different matched-ID distributions
within a batch were not tested. Full warming caches every primary rule on main;
partial warming caches only matching rules (zero or one). The PRs still publish
their full snapshot. These are explicit cache-population regimes, not a measured
production traffic distribution.

All percentages use equal-weight averages of **per-run means**, and separately
of per-run medians. These are not pooled distributions or confidence intervals.
Positive change means slower. Raw sample counts, deviations, p99 statistics and
caller-allocation observations are retained.

## Conclusions, including regressions

**No universal speedup or zero-regression claim is supported.**

At 10k rules, zero/one-match singleton means are **149.25–155.89% slower** on
the parent (about 2.5x main), and **16.96–27.03% slower** on the stacked child.
Parent five-event means remain **37.21–38.94% slower**; child five-event means
are **6.44–15.10% faster**. At ten events the parent is nearly flat to +4.55%
in mean but **8.87–10.46% slower in median**; child means improve **23.88–30.97%**.
At 100 events parent means improve **23.84–25.83%**, child **30.50–31.83%**.

Thus, **five is the first tested batch size where both sparse child shapes
improve in both mean and median**. The parent first does so at **100 among the
tested sizes**; its actual crossing between 10 and 100 is not located.
Two-event child behavior is mixed, not a universal break-even result.

Dense 10k/all-match/10-event means show **9.54x parent / 12.84x child** speedups,
but these are two-run dense synthetic results, not representative fleet gains.
The repeated 10k/eight/singleton case is about **5.72% slower parent** and
**2.93% faster child** in mean.

Cold 10k publication is **+63.33% mean / +68.66% median parent** and
**+30.39% mean / +33.99% median child**. At 1k parent costs +6.53%/+8.38%;
child improves 11.52%/11.24%. At 100 both are faster.

Concurrency is not uniformly improved. Across seven runs, eight-reader
singleton waves are **+23.60%/+30.16% child mean** for one/eight cached sources,
with about **+22.39%/+23.91% median**. A slow fourth child VM (roughly 21–22ms
versus earlier 8–9ms eight-reader singleton waves) contributes materially.
Two additional longer repetitions did not reproduce that broad slowdown, but
the full averages retain it. This is an observed cost/uncertainty, not a stable
causal overhead estimate or a universal concurrency pass. Ten-event waves
improve across the matrix; see all rows below.

Retained memory depends strongly on warming. For eight fully warmed 10k sources,
ETS delta falls from **148,209,200 bytes main** to **15,156,928 parent /
13,236,288 child**, with header payloads reported separately. For 32 partially
warmed 10k sources, ETS instead rises from **40,183,040 bytes main** to
**45,261,184 parent / 43,338,624 child**, plus **5,700,621 / 1,255,378 bytes**
of logical header payload. Do not characterize these as universal memory savings.

## Hot sparse routing: full cache

| Case | Runs/head | Main mean (us) | Parent mean (us) | Child mean (us) | Parent mean change | Child mean change | Parent median change | Child median change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 rules / one matches / 1 events | 5 | 11.49 | 10.77 | 10.69 | -6.29% | -6.99% | -10.09% | -12.66% |
| 100 rules / one matches / 10 events | 5 | 125.07 | 51.63 | 49.86 | -58.72% | -60.14% | -58.73% | -60.90% |
| 100 rules / one matches / 100 events | 5 | 1302.15 | 508.05 | 523.14 | -60.98% | -59.82% | -59.75% | -60.54% |
| 100 rules / one matches / 2 events | 5 | 24.09 | 16.06 | 16.09 | -33.34% | -33.23% | -32.64% | -34.68% |
| 100 rules / one matches / 5 events | 5 | 58.42 | 29.94 | 29.37 | -48.75% | -49.73% | -47.11% | -49.54% |
| 100 rules / zero matches / 1 events | 5 | 9.95 | 10.85 | 10.22 | +9.06% | +2.73% | +5.69% | -2.02% |
| 100 rules / zero matches / 10 events | 5 | 102.11 | 52.00 | 50.83 | -49.08% | -50.22% | -50.72% | -49.50% |
| 100 rules / zero matches / 100 events | 5 | 1135.32 | 508.95 | 498.34 | -55.17% | -56.11% | -54.44% | -55.88% |
| 100 rules / zero matches / 2 events | 5 | 20.76 | 16.15 | 15.96 | -22.20% | -23.15% | -22.70% | -24.16% |
| 100 rules / zero matches / 5 events | 5 | 50.14 | 29.14 | 29.25 | -41.88% | -41.67% | -41.45% | -41.87% |
| 1000 rules / one matches / 1 events | 5 | 102.14 | 106.44 | 110.52 | +4.21% | +8.21% | +4.16% | +4.85% |
| 1000 rules / one matches / 10 events | 5 | 1015.88 | 661.36 | 676.06 | -34.90% | -33.45% | -37.83% | -35.84% |
| 1000 rules / one matches / 100 events | 5 | 10088.53 | 5655.54 | 5852.61 | -43.94% | -41.99% | -44.18% | -44.07% |
| 1000 rules / one matches / 2 events | 5 | 205.02 | 151.34 | 148.46 | -26.18% | -27.58% | -29.43% | -29.95% |
| 1000 rules / one matches / 5 events | 5 | 507.97 | 401.42 | 406.86 | -20.98% | -19.91% | -27.31% | -29.08% |
| 1000 rules / zero matches / 1 events | 5 | 101.05 | 107.15 | 110.29 | +6.04% | +9.15% | +3.46% | +5.61% |
| 1000 rules / zero matches / 10 events | 5 | 978.17 | 676.54 | 667.53 | -30.84% | -31.76% | -35.54% | -35.67% |
| 1000 rules / zero matches / 100 events | 5 | 10567.59 | 5670.93 | 5777.74 | -46.34% | -45.33% | -46.25% | -44.59% |
| 1000 rules / zero matches / 2 events | 5 | 196.05 | 153.60 | 153.72 | -21.66% | -21.59% | -26.19% | -26.21% |
| 1000 rules / zero matches / 5 events | 5 | 498.98 | 393.06 | 403.11 | -21.23% | -19.21% | -28.16% | -25.34% |
| 10000 rules / one matches / 1 events | 5 | 1521.71 | 3893.89 | 1933.06 | +155.89% | +27.03% | +173.01% | +17.29% |
| 10000 rules / one matches / 10 events | 5 | 15107.50 | 15795.60 | 11500.55 | +4.55% | -23.88% | +10.46% | -27.65% |
| 10000 rules / one matches / 100 events | 5 | 150717.22 | 114782.88 | 102751.29 | -23.84% | -31.83% | -21.25% | -32.93% |
| 10000 rules / one matches / 2 events | 5 | 3489.09 | 5524.29 | 3450.09 | +58.33% | -1.12% | +88.25% | -2.55% |
| 10000 rules / one matches / 5 events | 5 | 7564.47 | 10510.14 | 6422.27 | +38.94% | -15.10% | +42.40% | -20.65% |
| 10000 rules / zero matches / 1 events | 5 | 1514.53 | 3774.99 | 1771.43 | +149.25% | +16.96% | +160.30% | +9.91% |
| 10000 rules / zero matches / 10 events | 5 | 16185.98 | 16094.03 | 11172.93 | -0.57% | -30.97% | +8.87% | -29.18% |
| 10000 rules / zero matches / 100 events | 5 | 149623.21 | 110969.29 | 103985.27 | -25.83% | -30.50% | -24.08% | -29.98% |
| 10000 rules / zero matches / 2 events | 5 | 2920.17 | 5286.63 | 3465.17 | +81.04% | +18.66% | +92.58% | +5.88% |
| 10000 rules / zero matches / 5 events | 5 | 7466.48 | 10245.03 | 6985.67 | +37.21% | -6.44% | +42.15% | -15.01% |

## Partial-cache sensitivity

| Case | Runs/head | Main mean (us) | Parent mean (us) | Child mean (us) | Parent mean change | Child mean change | Parent median change | Child median change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10000 rules / one matches / 1 events | 5 | 1516.04 | 3416.33 | 1904.88 | +125.35% | +25.65% | +119.73% | +18.56% |
| 10000 rules / one matches / 10 events | 5 | 15296.42 | 16145.14 | 11548.86 | +5.55% | -24.50% | +10.93% | -25.95% |
| 10000 rules / one matches / 100 events | 5 | 148899.99 | 110542.21 | 102130.91 | -25.76% | -31.41% | -23.36% | -30.69% |
| 10000 rules / zero matches / 1 events | 5 | 1479.86 | 3574.50 | 2075.62 | +141.54% | +40.26% | +135.86% | +30.08% |
| 10000 rules / zero matches / 10 events | 5 | 14839.99 | 15387.29 | 12353.41 | +3.69% | -16.76% | +9.44% | -20.77% |
| 10000 rules / zero matches / 100 events | 5 | 146651.20 | 110278.08 | 103215.94 | -24.80% | -29.62% | -22.88% | -29.60% |

## Dense sanity

| Case | Runs/head | Main mean (us) | Parent mean (us) | Child mean (us) | Parent mean change | Child mean change | Parent median change | Child median change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 rules / all matches / 1 events | 2 | 131.63 | 18.93 | 15.35 | -85.62% | -88.34% | -85.39% | -88.51% |
| 100 rules / all matches / 10 events | 2 | 1378.96 | 160.28 | 121.02 | -88.38% | -91.22% | -89.00% | -91.86% |
| 100 rules / eight matches / 1 events | 2 | 13.21 | 6.07 | 6.12 | -54.05% | -53.67% | -57.81% | -63.62% |
| 100 rules / eight matches / 10 events | 2 | 140.74 | 21.97 | 13.80 | -84.39% | -90.19% | -85.22% | -90.60% |
| 10000 rules / all matches / 1 events | 2 | 25064.70 | 6318.85 | 3030.01 | -74.79% | -87.91% | -75.46% | -88.98% |
| 10000 rules / all matches / 10 events | 2 | 372360.80 | 39025.31 | 29006.81 | -89.52% | -92.21% | -89.51% | -92.33% |
| 10000 rules / eight matches / 1 events | 5 | 354.36 | 374.62 | 343.98 | +5.72% | -2.93% | +41.13% | -2.36% |
| 10000 rules / eight matches / 10 events | 2 | 3433.48 | 771.31 | 706.15 | -77.54% | -79.43% | -75.88% | -80.55% |

## Empty / one-rule guards

| Case | Runs/head | Main mean (us) | Parent mean (us) | Child mean (us) | Parent mean change | Child mean change | Parent median change | Child median change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 rules / one matches / 1 events | 5 | 1.11 | 0.86 | 0.89 | -23.04% | -19.86% | -28.55% | -27.38% |
| 0 rules / one matches / 100 events | 2 | 103.10 | 3.56 | 3.48 | -96.55% | -96.62% | -96.54% | -96.61% |
| 0 rules / zero matches / 1 events | 5 | 1.09 | 0.86 | 0.90 | -21.10% | -16.86% | -29.42% | -27.08% |
| 0 rules / zero matches / 100 events | 2 | 102.75 | 3.64 | 3.67 | -96.46% | -96.43% | -96.81% | -96.96% |
| 1 rules / one matches / 1 events | 5 | 2.76 | 1.46 | 1.38 | -47.02% | -49.94% | -46.67% | -40.84% |
| 1 rules / one matches / 100 events | 2 | 313.56 | 30.16 | 27.02 | -90.38% | -91.38% | -90.51% | -91.40% |
| 1 rules / zero matches / 1 events | 5 | 1.58 | 1.26 | 1.30 | -20.50% | -17.94% | -28.28% | -24.32% |
| 1 rules / zero matches / 100 events | 2 | 133.23 | 19.68 | 21.35 | -85.23% | -83.98% | -86.01% | -85.48% |

Hot measures cached matching/preparation/target resolution at the production
matching boundary. It excludes fixture construction, parsing, DB, publication,
SourceRouter backend dispatch and sink I/O. It is not ingestion throughput.

## Concurrent hot routing

| Case | Runs/head | Main mean (us) | Parent mean (us) | Child mean (us) | Parent mean change | Child mean change | Parent median change | Child median change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 readers / 1 sources / 1 events | 5 | 2834.47 | 2944.39 | 2756.26 | +3.88% | -2.76% | +6.81% | -5.61% |
| 1 readers / 1 sources / 10 events | 5 | 25392.69 | 14369.38 | 14559.49 | -43.41% | -42.66% | -43.45% | -43.00% |
| 1 readers / 8 sources / 1 events | 5 | 2866.06 | 2564.25 | 2692.07 | -10.53% | -6.07% | -10.35% | -6.05% |
| 1 readers / 8 sources / 10 events | 5 | 25791.75 | 14407.58 | 15015.46 | -44.14% | -41.78% | -43.84% | -42.44% |
| 32 readers / 1 sources / 1 events | 5 | 30097.77 | 29524.47 | 33241.52 | -1.90% | +10.45% | +5.50% | +15.59% |
| 32 readers / 1 sources / 10 events | 5 | 230785.48 | 126607.76 | 150320.56 | -45.14% | -34.87% | -43.59% | -33.01% |
| 32 readers / 8 sources / 1 events | 5 | 28633.64 | 30561.87 | 32829.56 | +6.73% | +14.65% | +10.41% | +16.56% |
| 32 readers / 8 sources / 10 events | 5 | 225000.38 | 136696.76 | 165544.64 | -39.25% | -26.42% | -40.90% | -30.31% |
| 8 readers / 1 sources / 1 events | 7 | 8159.40 | 8266.70 | 10084.87 | +1.32% | +23.60% | +0.37% | +22.39% |
| 8 readers / 1 sources / 10 events | 7 | 54973.26 | 33735.24 | 41601.70 | -38.63% | -24.32% | -37.89% | -24.41% |
| 8 readers / 8 sources / 1 events | 7 | 8478.97 | 8450.85 | 11036.05 | -0.33% | +30.16% | +0.36% | +23.91% |
| 8 readers / 8 sources / 10 events | 7 | 57598.47 | 34621.77 | 42871.09 | -39.89% | -25.57% | -38.57% | -25.95% |

Each timed wave spawns/gates readers, then waits for all readers to complete their
production matching batches. Tasks receive only source/events, not parsed-rule
fixtures. Mean/p99 are **whole-wave elapsed time**, not per-reader percentiles.
Throughput is `readers * events / wave seconds`; task/barrier overhead is included
identically. With one reader/eight sources, only the first source is active and
the others establish background cache occupancy. Remaining sources can retain
disposable acceleration after header clearing; this is not an RSS measurement.

## Synthetic cold publication

| Case | Runs/head | Main mean (us) | Parent mean (us) | Child mean (us) | Parent mean change | Child mean change | Parent median change | Child median change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 rules / eight matches / 1 events | 5 | 240.55 | 101.50 | 94.96 | -57.80% | -60.53% | -58.84% | -61.03% |
| 1000 rules / eight matches / 1 events | 5 | 812.90 | 866.00 | 719.23 | +6.53% | -11.52% | +8.38% | -11.24% |
| 10000 rules / eight matches / 1 events | 5 | 6850.39 | 11188.49 | 8932.05 | +63.33% | +30.39% | +68.66% | +33.99% |

Cold includes cleanup/invalidation/barrier, task creation/copying, building from
already parsed rules, encoding, registration and Cachex publication. It excludes
DB/parsing and is not the actual Courier load pipeline. Caller-memory values do
not include worker allocations and are not total publication allocations.

## Retained-memory populations

| Rules / warming / sources | Runs/head | Main ETS delta (bytes) | Parent ETS delta | Child ETS delta | Parent header binary payload | Child header binary payload |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 rules / full / 32 | 5 | 5905968 | 456832 | 435072 | 49310 | 15284 |
| 100 rules / representative / 32 | 5 | 447488 | 456832 | 435072 | 49310 | 15284 |
| 1000 rules / full / 32 | 5 | 58829360 | 4305792 | 4111232 | 531359 | 127431 |
| 1000 rules / representative / 32 | 5 | 3835648 | 4305792 | 4111232 | 531359 | 127431 |
| 10000 rules / full / 8 | 5 | 148209200 | 15156928 | 13236288 | 1428788 | 313857 |
| 10000 rules / representative / 32 | 5 | 40183040 | 45261184 | 43338624 | 5700621 | 1255378 |

Source limit is eight and store byte limit is explicitly unlimited to isolate
source-capacity pressure. Thirty-two sources pressure the store in all but the
full-10k row. That row uses eight sources to keep main below its 100k Cachex-entry
cap; it is not the same population as the 32-source partial row. Each head uses
identical populations and primary-cache warming within a row. All snapshots
continue routing correctly across three publication/read cycles and invalidation.

ETS deltas are after the final read minus the pre-publication baseline. Logical
header payloads are reported separately; main has no compressed snapshot header
but still has its other cache data/binaries. VM binary/total gauges and store
estimates are preserved in JSON. They include process/heap-capacity effects and
are **not RSS or isolated live-object totals**; negative binary deltas can result
from collection of unrelated startup allocations. Source invalidation removes
headers/acceleration, while main primary-rule entries can persist until ordinary
ID invalidation/expiration; complete cache clear is recorded separately.

## Actual loopback HTTP ingestion with PostgreSQL

| Case | Runs/head | Main mean (us) | Parent mean (us) | Child mean (us) | Parent mean change | Child mean change | Parent median change | Child median change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 rules / 1 events / HTTP | 5 | 1822.85 | 689.27 | 641.88 | -62.19% | -64.79% | -44.42% | -50.04% |
| 100 rules / 10 events / HTTP | 5 | 1330.39 | 1315.83 | 1111.81 | -1.09% | -16.43% | -4.82% | -16.58% |
| 100 rules / 100 events / HTTP | 5 | 4289.57 | 2911.69 | 3015.23 | -32.12% | -29.71% | -25.24% | -26.52% |
| 10000 rules / 1 events / HTTP | 5 | 3801.58 | 4362.49 | 4292.95 | +14.75% | +12.93% | +6.26% | +18.36% |
| 10000 rules / 10 events / HTTP | 5 | 28339.02 | 15487.50 | 15203.58 | -45.35% | -46.35% | -46.43% | -47.30% |
| 10000 rules / 100 events / HTTP | 5 | 265655.78 | 121099.47 | 121969.47 | -54.41% | -54.09% | -55.03% | -54.83% |

Real Phoenix TCP listener on **127.0.0.1:4143**, health checked before traffic.
Test sandbox metadata, dedicated temporary sources/sinks, single-tenant PostgreSQL
default backend, spool disabled, no backend/request mocks. Backend work runs
through actual buffering/Broadway/PostgreSQL. No external environment is targeted.
HTTP fixtures route one rule into the sink; microbenchmark sinks are not called.

Thirty measured requests/case plus five warmups, **2,700 measured requests** total.
Warmup primary/sink rows are drained before timing. Each measured phase waits for
exact primary and sink counts and distinct IDs, then owned source supervisors and
tables are removed and metadata rolled back. Request means include TCP, HTTP,
JSON/auth/validation/routing/enqueue work; payload UUID generation is outside the
individual request timer. Backend completion is recorded separately.

At 10k rules, singleton response means are **+14.75% parent / +12.93% child**,
medians **+6.26% / +18.36%**. Ten-event response means improve **45.35%/46.35%**,
100-event means **54.41%/54.09%**. Thirty-request plus backend-completion time for
100-event batches is **9.58s main / 5.21s parent / 5.22s child**. Smaller completion
windows are around two seconds and strongly affected by backend flush timing.
HTTP `p99_us` is the maximum of 30 samples per run, not a reliable production
tail percentile. This local serial test is not fleet capacity or production RPS.

## Diagnostics, reproduction and limits

The initial concurrency driver copied the parsed setup fixture into each task.
Those six VMs / 72 observations are retained under `superseded_diagnostics`;
they represent a different workload and are not used for routing comparisons.
No valid timing outlier was removed. Pre-timing compiler-permission failures
were retried only after confirming timing had not started; completed result files
were preserved. Failed setup smokes are not timing observations.

To reproduce, save all three drivers outside the checkout, set
`ROUTING_BENCH_LIBRARY_PATH` to the saved scale driver for extra/HTTP lanes,
and use `MIX_ENV=test ERL_FLAGS='+S 6:6'`. Run only in a disposable local app VM:
drivers clear the rules cache; retention restarts the disposable acceleration;
HTTP uses a test sandbox and its own loopback listener/tables.

Primary settings:
`ROUTING_BENCH_RULES=100,1000,10000`,
`ROUTING_BENCH_SHAPES=zero,one`,
`ROUTING_BENCH_BATCHES=1,2,5,10,100`,
`ROUTING_BENCH_SKIP_FOOTPRINT=1`.
Partial warming adds `ROUTING_BENCH_WARM_MODE=representative`.
Cold adds `ROUTING_BENCH_PUBLICATION=1`.
Extra lanes use `ROUTING_MAIN_MODE=concurrent|retained` and
`ROUTING_MAIN_REVISION=SHORT_SHA`; concurrency readers/sources are controlled by
`ROUTING_MAIN_READERS` and `ROUTING_MAIN_SOURCES`.
HTTP uses `ROUTING_HTTP_SAMPLES=30` and the default 1/10/100 batch sizes.
Every output uses an explicit `ROUTING_BENCH_OUTPUT` path. Exact run configuration
and revisions accompany each raw file in the aggregate.

No production workload-frequency measurements, full local suite, coverage run,
production traffic/rollout, main rebase, or application optimization is part of
this follow-up. Existing restore/miss guarantees and retention tradeoffs remain
documented separately. Results reveal merge risks; they do not silently fix them.
