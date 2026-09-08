# Routing snapshot cache hardening follow-up

This note records the measurements for the additive hardening commit on PR
#3943. It intentionally excludes the stacked zero-based positional-tree change.

The commit:

- prepares one immutable tree/snapshot for an ingest batch;
- stores compact `{rule_id, backend_id, sink}` targets rather than `%Rule{}`;
- rehydrates the current cache header once after an ETS generation is lost and
  otherwise carries a decoded reader-owned fallback for the rest of the batch;
- adds a configurable estimated-byte bound and store lifecycle telemetry.

## Environment and fixture

The environment and corrected six-case fixtures are the same as
[`source_routing_snapshot_results.md`](source_routing_snapshot_results.md):
Linux, Elixir 1.19.5, OTP 27.3.4.6, JIT enabled, Benchee with one second warmup,
three
seconds measurement, one second memory measurement, and outlier exclusion
disabled. Results are microbenchmarks rather than production throughput claims.

## Single-event routing

One local run of the checked-in `source_routing_bench.exs` produced:

| Rules / matches | #3943 published runs | Cache hardening | Approximate mean change |
| --- | ---: | ---: | ---: |
| 100 / 8 | 7.63 / 7.20 us | 7.02 us | 1.06x faster |
| 100 / 1 | 11.28 / 10.34 us | 11.65 us | comparable |
| 100 / all | 74.60 / 72.57 us | 24.78 us | 2.97x faster |
| 1,000 / 8 | 39.04 / 38.97 us | 35.97 us | 1.08x faster |
| 1,000 / 1 | 86.07 / 86.89 us | 129.17 us | 1.49x slower |
| 1,000 / all | 0.88 / 0.90 ms | 0.348 ms | 2.56x faster |

The 1,000-rule/one-match result is disclosed rather than generalized away. The
adjacent representation probe documented by the positional follow-up found the
position-vs-ID matching phase within 1%, so this shape needs production-informed
profiling rather than another speculative tree change.

## Batch reuse

The checked-in batch benchmark builds a 1,000-rule snapshot with eight matches
and routes the same event 10 or 100 times. It compares calling the public router
for every event with preparing once and carrying the immutable state through the
batch.

| Batch size | Prepare once | Fetch per event | Speedup | Allocation reduction |
| --- | ---: | ---: | ---: | ---: |
| 10 events | 90.78 us | 393.89 us | 4.34x | 8.85x |
| 100 events | 0.66 ms | 3.87 ms | 5.85x | 11.33x |

The positional child reduces the prepare-once allocations further, but those
additional results are not attributed to this commit.

## Compact target footprint

A component model counted one externalized tree, sorted snapshot tuple, binary
ID index, and compressed reader-owned fallback for both representations. It
used the real six database-backed benchmark fixtures.

| Rules / shape | Full `%Rule{}` snapshot | Compact ID/target snapshot | Reduction |
| --- | ---: | ---: | ---: |
| 100 / 8 | 119,678 B | 6,250 B | 94.8% |
| 100 / 1 | 141,660 B | 6,936 B | 95.1% |
| 100 / all | 135,339 B | 5,757 B | 95.7% |
| 1,000 / 8 | 1,200,859 B | 65,572 B | 94.5% |
| 1,000 / 1 | 1,415,404 B | 71,333 B | 95.0% |
| 1,000 / all | 1,352,493 B | 58,385 B | 95.7% |

This is a representation model, not resident-process memory. It excludes
Cachex/ETS table overhead and allocator fragmentation equally from both sides.
The measured compact snapshot publication step ranged from 0.42 to 0.57 ms for
the 1,000-rule cases.

## Commands

```sh
MIX_ENV=test ../bin/x mix run test/profiling/source_routing_bench.exs
MIX_ENV=test ../bin/x mix run test/profiling/source_routing_batch_bench.exs
MIX_ENV=test ../bin/x mix run /tmp/routing-base-footprint.exs
```
