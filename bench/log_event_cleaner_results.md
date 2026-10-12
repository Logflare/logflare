# Payload cleaner change-tracking benchmark follow-up

## Revisions and methodology

- Current-main baseline: `001d1ef2289e`.
- Pre-fix PR: `1493849f2afa` (same cleaner as `b116704d62d6`).
- Explicit change-tracking fix: `f92a6421`; benchmark/report-only follow-up does not change runtime code.
- Linux, Elixir 1.19.5, OTP 27, 6 online schedulers; Benchee parallelism 1.
- Same VM, pre-generated identical inputs, baseline source compiled under alternate module names. Main/pre-fix/fixed outputs checked for exact equality before measurement.
- Two sequential runs per scenario, median throughput and allocation. Cleaner matrix: 1s measurement, 0s warmup, 1s allocation measurement. Different batches ran sequentially, never concurrently. 64-key cases appearing in more than one batch use their four-sample median.
- Short microbenchmarks have scheduler/GC noise; small throughput differences are not conclusive. Allocation figures and the large depth-scaling difference are much more stable.

## Decisions

Explicit change flags remove the quadratic subtree comparisons. On a dirty map chain, fixed depth 64 averages about 2.7 us instead of 34 us, and depth 512 about 26 us instead of 2071 us. Depth 512 is approximately 80x faster than the pre-fix PR; fixed scaling is linear. The pure dirty map chain still has modest overhead versus main (up to about 13% slower and 18% more allocation); alternating map/list chains generally improve throughput but allocate about 29% more than main.

Clean and sparse payloads retain large gains. Dense dirty/unsafe small maps remain a trade-off, now covered explicitly: tiny mixed dirty 4 is about 11% slower / 48% more allocation than main. Some flat-map cases with many mutations regress more significantly; see the full matrix rather than treating this as a universally faster cleaner.

Unconditional rebuild cutoffs of 4 and 16 entries were prototyped, not shipped. Cutoff 4 reduced tiny mixed allocation by about 28% versus the fixed cleaner, but provided no meaningful speed gain, slowed clean 4 by about 35%, slowed realistic cleaner fixture shapes by 11–28%, and slowed clean lists by about 36% while increasing their allocation 40%. Cutoff 16 penalized clean/sparse maps and realistic shapes even more. A size-only cutoff therefore sacrifices common-path gains for dense-dirty cases; no arbitrary small-map specialization was adopted.

## Cleaner-only matrix

The seven fixture rows measure only cleaning, not full ingestion. All 60 versioned synthetic scenarios and seven realistic cleaner fixture shapes were measured.

| Input | Main K ops/s | Fixed K ops/s | Throughput vs main | Allocation vs main | Throughput vs pre-fix |
|---|---:|---:|---:|---:|---:|
| depth: dirty map 128 | 178.27 | 171.27 | -3.9% | +18.2% | +2048.1% |
| depth: dirty map 256 | 85.40 | 78.99 | -7.5% | +18.3% | +3718.0% |
| depth: dirty map 32 | 694.25 | 651.02 | -6.2% | +17.5% | +478.0% |
| depth: dirty map 512 | 44.21 | 38.60 | -12.7% | +18.2% | +7894.9% |
| depth: dirty map 64 | 382.58 | 371.21 | -3.0% | +18.3% | +1162.9% |
| depth: dirty map 8 | 2376.09 | 2373.59 | -0.1% | +16.8% | +107.8% |
| depth: dirty map/list 128 | 126.53 | 137.53 | +8.7% | +29.1% | +2177.6% |
| depth: dirty map/list 256 | 62.53 | 64.75 | +3.6% | +29.3% | +4085.7% |
| depth: dirty map/list 32 | 489.52 | 549.03 | +12.2% | +28.0% | +599.0% |
| depth: dirty map/list 512 | 28.70 | 32.66 | +13.8% | +29.1% | +8134.0% |
| depth: dirty map/list 64 | 250.29 | 294.39 | +17.6% | +29.1% | +1146.8% |
| depth: dirty map/list 8 | 2276.04 | 2241.81 | -1.5% | +28.4% | +135.3% |
| fixture: log Cloudflare | 139.51 | 449.84 | +222.4% | -54.5% | -0.2% |
| fixture: log Observer | 242.51 | 820.61 | +238.4% | -52.2% | +3.9% |
| fixture: log Phoenix | 604.01 | 1625.87 | +169.2% | -47.9% | +3.2% |
| fixture: metric | 1358.65 | 2517.09 | +85.3% | -40.8% | -1.7% |
| fixture: trace Auth | 499.88 | 1060.72 | +112.2% | -43.3% | +6.2% |
| fixture: trace Gateway | 519.03 | 1099.47 | +111.8% | -43.2% | +2.6% |
| fixture: trace Realtime | 350.89 | 817.33 | +132.9% | -43.9% | -2.5% |
| list: clean 64 maps | 245.39 | 345.41 | +40.8% | -24.3% | +5.3% |
| list: half empty | 733.14 | 942.28 | +28.5% | -10.4% | +4.7% |
| list: one nested change | 250.50 | 339.08 | +35.4% | -23.5% | +2.7% |
| map: all empty 1 | 22096.52 | 17933.47 | -18.8% | +150.0% | -11.9% |
| map: all empty 16 | 10743.34 | 10217.87 | -4.9% | +0.0% | +8.7% |
| map: all empty 32 | 5195.60 | 5318.08 | +2.4% | +0.0% | +10.1% |
| map: all empty 33 | 3842.17 | 3969.62 | +3.3% | +0.0% | -5.0% |
| map: all empty 4 | 19110.89 | 9211.40 | -51.8% | +233.3% | +4.2% |
| map: all empty 64 | 2177.90 | 2524.15 | +15.9% | +0.0% | -0.9% |
| map: all empty 8 | 10386.59 | 6579.52 | -36.7% | +282.4% | +13.8% |
| map: all unsafe 1 | 3549.50 | 4317.90 | +21.6% | +16.4% | +1.2% |
| map: all unsafe 16 | 225.20 | 145.88 | -35.2% | +86.0% | +5.8% |
| map: all unsafe 32 | 87.35 | 44.64 | -48.9% | +129.5% | +5.7% |
| map: all unsafe 33 | 71.94 | 7.85 | -89.1% | +175.7% | -4.7% |
| map: all unsafe 4 | 1186.37 | 938.73 | -20.9% | +26.6% | +0.8% |
| map: all unsafe 64 | 51.12 | 52.76 | +3.2% | +22.9% | -2.3% |
| map: all unsafe 8 | 557.03 | 401.88 | -27.9% | +40.1% | +8.1% |
| map: clean 1 | 12460.15 | 14827.31 | +19.0% | -13.6% | +11.1% |
| map: clean 16 | 614.90 | 2684.95 | +336.6% | -53.7% | +30.2% |
| map: clean 32 | 164.89 | 1262.87 | +665.9% | -70.0% | -2.1% |
| map: clean 33 | 156.54 | 1129.56 | +621.6% | -70.9% | +6.5% |
| map: clean 4 | 4681.32 | 6898.00 | +47.4% | -37.1% | +1.4% |
| map: clean 64 | 140.03 | 665.19 | +375.0% | -66.3% | -1.8% |
| map: clean 8 | 1627.03 | 4362.19 | +168.1% | -48.0% | +42.3% |
| map: collision 66 | 57.59 | 64.92 | +12.7% | +2.2% | -0.1% |
| map: half empty 1 | 12968.04 | 14944.30 | +15.2% | -13.6% | +14.7% |
| map: half empty 16 | 1592.78 | 1889.19 | +18.6% | +102.6% | +20.3% |
| map: half empty 32 | 551.55 | 775.35 | +40.6% | +122.4% | +14.1% |
| map: half empty 33 | 583.77 | 273.79 | -53.1% | +78.1% | -2.9% |
| map: half empty 4 | 7300.93 | 8833.40 | +21.0% | +22.6% | +33.4% |
| map: half empty 64 | 197.74 | 272.82 | +38.0% | -20.9% | -0.3% |
| map: half empty 8 | 3866.18 | 3899.05 | +0.9% | +19.5% | +8.8% |
| map: mostly dirty 1 | 19702.76 | 19885.07 | +0.9% | +150.0% | +12.1% |
| map: mostly dirty 16 | 1159.03 | 518.69 | -55.2% | +233.8% | -11.7% |
| map: mostly dirty 32 | 498.52 | 228.16 | -54.2% | +375.3% | -1.7% |
| map: mostly dirty 33 | 449.07 | 110.25 | -75.4% | +309.9% | -15.6% |
| map: mostly dirty 4 | 2961.82 | 2563.42 | -13.5% | +62.7% | +2.6% |
| map: mostly dirty 64 | 175.93 | 78.77 | -55.2% | +181.4% | -11.9% |
| map: mostly dirty 8 | 2276.25 | 1325.14 | -41.8% | +97.8% | +6.2% |
| map: sparse dirty 1 | 22128.37 | 19520.22 | -11.8% | +150.0% | +2.7% |
| map: sparse dirty 16 | 678.82 | 2480.23 | +265.4% | -42.9% | +12.0% |
| map: sparse dirty 32 | 169.37 | 1156.45 | +582.8% | -64.3% | -11.3% |
| map: sparse dirty 33 | 193.65 | 312.90 | +61.6% | -64.3% | -15.0% |
| map: sparse dirty 4 | 6135.33 | 7342.83 | +19.7% | -13.5% | +25.5% |
| map: sparse dirty 64 | 130.16 | 676.21 | +419.5% | -65.2% | +3.6% |
| map: sparse dirty 66 | 133.24 | 543.19 | +307.7% | -63.3% | -2.6% |
| map: sparse dirty 8 | 2039.41 | 4444.99 | +118.0% | -34.0% | +8.6% |
| map: tiny mixed dirty 4 | 2507.35 | 2233.20 | -10.9% | +47.7% | +3.6% |

## LogEvent.make/2: seven shapes

Same current-main LogEvent implementation for all jobs, changing only its cleaner; no app startup or DB writes. Sources use `validate_schema: false` and have no copy/drop/KV transforms. The fixture generator module is extracted without executing its DB-inserting benchmark setup. Before measurement, generated IDs/timestamps are fixed only for an exact output check; benchmark jobs retain normal ID/timestamp generation. Two runs, 2s measurement / 1s warmup / 1s allocation per job/input.

Across the seven shapes, throughput geometric mean is **+61.3%**, allocation geometric mean **-41.6%**, versus main. These controlled measurements do not include enabled schema validation, source transforms, backend ingestion or service contention, and are not directly interchangeable with the older DB-backed suite's absolute numbers.

| Input | Main K ops/s | Fixed K ops/s | Throughput vs main | Allocation vs main | Throughput vs pre-fix |
|---|---:|---:|---:|---:|---:|
| log Cloudflare | 97.63 | 252.40 | +158.5% | -51.8% | +1.3% |
| log Observer | 146.25 | 322.85 | +120.8% | -49.5% | -13.8% |
| log Phoenix | 274.82 | 414.03 | +50.7% | -40.6% | -5.1% |
| metric | 401.60 | 493.93 | +23.0% | -27.3% | -6.0% |
| trace Auth | 240.16 | 333.14 | +38.7% | -38.6% | -15.3% |
| trace Gateway | 259.57 | 352.70 | +35.9% | -39.0% | -4.5% |
| trace Realtime | 210.34 | 299.60 | +42.4% | -40.7% | -15.0% |

## Reproduction and artifacts

The versioned density/depth script can be run unchanged on comparison revisions:

```sh
MIX_ENV=test LF_BENCH_TIME=3 LF_BENCH_WARMUP=1 LF_BENCH_MEMORY_TIME=2 \
  LF_BENCH_OUTPUT=/tmp/cleaner-results.json \
  ../bin/x mix run --no-start bench/log_event_cleaner.exs
```

`LF_BENCH_FILTER` accepts a regular expression, for example `^depth:` or `^map:.* (1|4|8|16)$`. Unmatched filters fail rather than silently benchmarking nothing.

Local paired-run harnesses: `/tmp/pr3974-fixed-comparison.exs`, `/tmp/pr3974-fixed-end-to-end.exs`, `/tmp/pr3974-small-strategy.exs`. Raw JSON: `/tmp/pr3974-fixed-{depth,original,small,large,end-to-end}-{1,2}.json` and `/tmp/pr3974-small-strategy-{1,2}.json`. Standalone filtered script and JSON output smoke-tested separately.

## Compatibility and quality gates

- Complete transformer, metadata cleaner and LogEvent suites: 7 properties, 109 tests, 0 failures.
- 20,000 deterministic exact differential cases against main and sequential legacy cleaning/key normalization pass.
- New tests cover depth-512 dirty maps and alternating map/list chains, unchanged-map/subtree structural sharing, list deletions, nested key renames, structs, map-size boundaries and collision fallback.
- Formatting, warnings-as-errors compilation, lint, duplication and structure checks pass. Existing informational design suggestions remain; no ratchet was raised.
- No full test suite or coverage run. Hosted CI must be assessed separately.

