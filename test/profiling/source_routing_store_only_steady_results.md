# Store-only routing: steady-state and cold-publication follow-up

## Scope and revisions

This supplements [store-only restore results](source_routing_restore_results.md)
with hot-routing and synthetic cold-publication measurements. It does not replace
the miss-latency/retained-header memory tradeoff report.

- Parent before: `acc8f99f`; store-only parent: `bfc70119`.
- Positional child before: `229bb1d0`; store-only child: `1ea29092`.
- Subsequent documentation commits and the benchmark input filter do not change
  application runtime code. No original main-baseline benchmark was rerun.
- Raw per-run statistics, deviations, allocation observations, configuration and
  equal-weight aggregates: [source_routing_store_only_steady_results.json](source_routing_store_only_steady_results.json).

## Method

Identical branch-neutral `source_routing_scale_bench.exs` timed paths throughout,
saved outside the working copy before switching revisions. Linux, Elixir 1.19.5,
OTP 27.3.4.6/JIT, Benchee 1.5.0, six schedulers, parallel 1. CPU model unavailable.

Two hot matrices per revision: 100 and 10,000 rules, zero/one/eight/all matches,
1/10/100 events. The 10k x 100 dense case is omitted under the 100,000-returned-
target cap, leaving **23 cases per run**. Cold runs initially cover 100/1,000/10,000
rules, the eight-match shape and one synthetic publication.

Initial order: parent before/after, child before/after, child after/before, parent
after/before. Each fresh VM uses warmup 1s/time 3s/memory 1s. Setup validates the
exact normalized routing targets before timing; batches repeat the same fixture
event and thread the production stateful batch API.

Longer confirmations use warmup 2s/time 5s/memory 1s and before/after then
after/before order for each boundary:

- Parent hot: 100-rule eight-match cases at 1/10/100 events, plus 10k zero/one event.
- Child hot: 100-rule zero/one event, 10k one-match/10 events and all-match/1/10 events.
- Parent cold: 1,000 and 10,000 rules; child cold: 10,000 rules.

Two additional parent before/after, after/before pairs isolate the 10k zero-match
single-event and 10k cold cases with warmup 5s/time 10s/memory 1s. Only input
selection is added to the driver, outside every timed region. The input filter is
now checked into the harness for reproduction.

Total: **220 hot and 40 cold case observations**, across 40 fresh benchmark VMs.
No runs or outliers were discarded. Each table uses an equal-weight average of
all available **per-run means**, and separately an equal-weight average of per-run
medians. These are not pooled distributions or confidence intervals; different
confirmation durations and run counts are recorded in the raw artifact.

Hot timing includes preparing the cached tree/snapshot and matching/resolving the
batch; it excludes fixtures, parsing, DB, publication and sink I/O. Cold timing
includes invalidation/cleanup/barrier, task creation, building from already parsed
rules, encoding, registration and Cachex publication; it excludes DB/parsing and
is not the production Courier pipeline. Cleanup and assertions are identical at
each boundary. All runs are local and serial, not hosted CI or production traffic.

## Conclusions and measured costs

- The 10k/eight-match/100-event hot case is close: parent **+0.67% mean / -0.16%
  median**, child **-0.11% mean / -0.13% median**.
- Small-source costs are not zero. Parent 100-rule/eight-match cases are
  **+5.56% to +7.85% mean**, **+2.98% to +4.91% median**, over all four runs.
  Longer repetitions retain a positive cost; single-event deviations are large,
  so a precise causal/production overhead estimate is not claimed.
- Child 100-rule/zero-match/single-event is **+6.92% mean / +11.16% median** over
  four runs (longer subset: +5.20% mean / +9.58% median). This is a measured
  regression, not hidden by the larger-workload averages.
- Parent 10k zero/single is variable: all six runs average **-0.05% mean / +5.00%
  median**, but the isolated longer subset is **+4.70% mean / +15.98% median**.
  Per-run deviations in that subset are about 37–41%. The earlier improvement
  outlier did not establish a speedup; a no-regression claim is not supported.
- Child dense and one-match/10-event spikes did not consistently reproduce in
  the longer subset. All available 10k child dense runs are still **+3.81%**
  (one event) and **+3.02%** (10 events) in mean, with **+0.39%/+1.87%** median.
  Longer-subset means are +0.30%/-1.36%; report all results, not just confirmations.
- Cold timing is dominated by occasional outliers on both sides. At 10k,
  parent all-six aggregate is **-3.85% mean / -0.88% median**; child all-four is
  **+8.21% mean / +0.19% median**. The child's initial candidate outlier had
  56.9% deviation; the longer subset is -0.50% mean/-2.43% median. The parent's
  isolated baseline outlier had 100.6% deviation. Neither cold means nor the
  apparent aggregate improvement establish a reliable universal change.

This checks the current hot/cold boundaries, but **does not establish universal
zero-regression, a production-throughput gain or removal of the existing sparse
single-event regression versus main**. Memory retention after dropping header
retirement and the parent's slower sparse reader-plus-store miss completion remain
explicit costs in the separate restore report.

## Parent steady-state: all available runs

| Rules / matches / events | Runs per side | Before mean (us) | After mean (us) | Mean change | Median change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 100 / all / 1 | 2 | 19.86 | 19.61 | -1.27% | +3.45% |
| 100 / all / 10 | 2 | 167.03 | 163.08 | -2.36% | -2.03% |
| 100 / all / 100 | 2 | 1647.26 | 1614.81 | -1.97% | -2.41% |
| 100 / eight / 1 | 4 | 6.23 | 6.72 | +7.85% | +2.98% |
| 100 / eight / 10 | 4 | 20.31 | 21.65 | +6.57% | +4.91% |
| 100 / eight / 100 | 4 | 190.36 | 200.95 | +5.56% | +4.38% |
| 100 / one / 1 | 2 | 11.14 | 10.40 | -6.69% | -2.67% |
| 100 / one / 10 | 2 | 54.29 | 53.56 | -1.34% | -3.08% |
| 100 / one / 100 | 2 | 604.86 | 516.31 | -14.64% | -5.15% |
| 100 / zero / 1 | 2 | 10.77 | 10.68 | -0.89% | +0.22% |
| 100 / zero / 10 | 2 | 51.29 | 49.19 | -4.09% | -2.98% |
| 100 / zero / 100 | 2 | 519.33 | 499.97 | -3.73% | -2.42% |
| 10000 / all / 1 | 2 | 6574.20 | 6439.85 | -2.04% | -2.90% |
| 10000 / all / 10 | 2 | 39886.86 | 39942.60 | +0.14% | +0.88% |
| 10000 / eight / 1 | 2 | 377.55 | 376.92 | -0.17% | +1.51% |
| 10000 / eight / 10 | 2 | 778.25 | 767.70 | -1.36% | -0.88% |
| 10000 / eight / 100 | 2 | 4715.21 | 4746.58 | +0.67% | -0.16% |
| 10000 / one / 1 | 2 | 3731.05 | 3786.35 | +1.48% | +1.28% |
| 10000 / one / 10 | 2 | 16978.62 | 15870.76 | -6.53% | -4.73% |
| 10000 / one / 100 | 2 | 113174.77 | 113484.58 | +0.27% | -0.90% |
| 10000 / zero / 1 | 6 | 3640.63 | 3638.81 | -0.05% | +5.00% |
| 10000 / zero / 10 | 2 | 15789.85 | 15766.97 | -0.14% | -0.52% |
| 10000 / zero / 100 | 2 | 124275.34 | 112691.73 | -9.32% | -3.81% |

## Positional child steady-state: all available runs

| Rules / matches / events | Runs per side | Before mean (us) | After mean (us) | Mean change | Median change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 100 / all / 1 | 2 | 15.40 | 15.21 | -1.20% | +3.74% |
| 100 / all / 10 | 2 | 122.33 | 116.05 | -5.13% | -2.32% |
| 100 / all / 100 | 2 | 1239.02 | 1179.40 | -4.81% | -3.03% |
| 100 / eight / 1 | 2 | 4.99 | 5.15 | +3.16% | -1.84% |
| 100 / eight / 10 | 2 | 14.79 | 13.82 | -6.52% | -1.90% |
| 100 / eight / 100 | 2 | 114.38 | 117.46 | +2.70% | -1.29% |
| 100 / one / 1 | 2 | 10.58 | 10.33 | -2.32% | -8.18% |
| 100 / one / 10 | 2 | 50.72 | 49.85 | -1.72% | -1.83% |
| 100 / one / 100 | 2 | 514.15 | 510.04 | -0.80% | -0.91% |
| 100 / zero / 1 | 4 | 10.42 | 11.14 | +6.92% | +11.16% |
| 100 / zero / 10 | 2 | 50.91 | 51.27 | +0.70% | +2.26% |
| 100 / zero / 100 | 2 | 503.60 | 484.53 | -3.79% | -2.78% |
| 10000 / all / 1 | 4 | 3046.72 | 3162.94 | +3.81% | +0.39% |
| 10000 / all / 10 | 4 | 29872.88 | 30776.02 | +3.02% | +1.87% |
| 10000 / eight / 1 | 2 | 333.70 | 334.94 | +0.37% | -0.74% |
| 10000 / eight / 10 | 2 | 712.15 | 732.93 | +2.92% | -0.32% |
| 10000 / eight / 100 | 2 | 4530.22 | 4525.08 | -0.11% | -0.13% |
| 10000 / one / 1 | 2 | 1775.11 | 1799.34 | +1.37% | +1.12% |
| 10000 / one / 10 | 4 | 10966.33 | 11347.17 | +3.47% | +2.02% |
| 10000 / one / 100 | 2 | 93991.14 | 94114.14 | +0.13% | -1.07% |
| 10000 / zero / 1 | 2 | 1784.63 | 1794.84 | +0.57% | +0.11% |
| 10000 / zero / 10 | 2 | 10981.86 | 10604.82 | -3.43% | -3.38% |
| 10000 / zero / 100 | 2 | 95012.78 | 91817.04 | -3.36% | -4.26% |

## Synthetic cold publication: all available runs

### Parent

| Rules / matches / events | Runs per side | Before mean (us) | After mean (us) | Mean change | Median change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 100 / eight / 1 | 2 | 104.25 | 100.07 | -4.01% | -4.09% |
| 1000 / eight / 1 | 4 | 901.43 | 902.29 | +0.09% | -3.33% |
| 10000 / eight / 1 | 6 | 11703.73 | 11252.73 | -3.85% | -0.88% |

### Positional child

| Rules / matches / events | Runs per side | Before mean (us) | After mean (us) | Mean change | Median change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 100 / eight / 1 | 2 | 94.97 | 95.29 | +0.34% | -0.02% |
| 1000 / eight / 1 | 2 | 687.44 | 696.59 | +1.33% | +0.39% |
| 10000 / eight / 1 | 4 | 8393.10 | 9082.55 | +8.21% | +0.19% |

Cold Benchee caller-memory observations do not include the worker's allocations
and must not be interpreted as total publication allocation. Hot allocation
statistics are retained per run in JSON; they are caller allocation observations,
not retained heap or RSS, and small-case measurements varied. This follow-up does
not replace the explicitly separate retained-memory measurements.

## Reproduction

Save the current harness outside the working copy and use it unchanged at each
revision. Never run it against a live application VM: its cleanup clears the local
rules cache and deletes local acceleration.

```sh
cp test/profiling/source_routing_scale_bench.exs /tmp/routing-scale.exs

# Hot matrix (23 cases)
MIX_ENV=test ERL_FLAGS='+S 6:6' ROUTING_BENCH_RULES=100,10000 \
  ROUTING_BENCH_OUTPUT=/tmp/hot-REV-r1.json \
  ../bin/x mix run /tmp/routing-scale.exs

# Cold (three sizes)
MIX_ENV=test ERL_FLAGS='+S 6:6' ROUTING_BENCH_PUBLICATION=1 \
  ROUTING_BENCH_OUTPUT=/tmp/cold-REV-r1.json \
  ../bin/x mix run /tmp/routing-scale.exs

# Example isolated confirmation
MIX_ENV=test ERL_FLAGS='+S 6:6' ROUTING_BENCH_RULES=10000 \
  ROUTING_BENCH_INPUT_FILTER='^10000 rules / zero matches / 1 events$' \
  ROUTING_BENCH_WARMUP=5 ROUTING_BENCH_TIME=10 \
  ROUTING_BENCH_OUTPUT=/tmp/isolated-REV.json \
  ../bin/x mix run /tmp/routing-scale.exs
```

Before editing/publishing this report, the exact application revisions named above
were independently benchmarked. The only repository edits for this follow-up are
documentation/raw artifacts and an optional benchmark-input selector. Previously
validated application tests remain applicable; no application code, test assertion,
gate budget or published history is changed.
