# Positional routing snapshot follow-up

This note isolates the stacked zero-based positional-tree change from the cache
hardening included in PR #3943. The parent already owns compact targets, batch
reuse, missing-generation repair, byte-aware capacity, and lifecycle telemetry.

The child changes only how the tree addresses those compact targets:

- build the tree and ordered target tuple from the same rule ordering;
- emit zero-based tuple positions instead of database rule IDs;
- remove the binary ID index and binary search;
- read sparse target tuple elements directly and return the whole tuple on dense
  matches instead of rebuilding an ID-keyed map.

## Environment and fixture

The environment and corrected six-case fixtures are the same as
[`source_routing_snapshot_results.md`](source_routing_snapshot_results.md):
Linux, Elixir 1.19.5, OTP 27.3.4.6, JIT enabled, Benchee with two seconds warmup, five
seconds measurement, two seconds memory measurement, and outlier exclusion
disabled. Results are microbenchmarks rather than production throughput claims.

## Single-event comparison with the ID-keyed parent

One adjacent run per representation produced:

| Rules / matches | ID-keyed parent | Positional child | Approximate change |
| --- | ---: | ---: | ---: |
| 100 / 8 | 7.02 us | 5.92 us | 1.19x faster |
| 100 / 1 | 11.65 us | 10.60 us | 1.10x faster |
| 100 / all | 24.78 us | 19.11 us | 1.30x faster |
| 1,000 / 8 | 35.97 us | 33.92 us | 1.06x faster |
| 1,000 / 1 | 129.17 us | 115.80 us | 1.12x faster |
| 1,000 / all | 347.55 us | 264.88 us | 1.31x faster |

The sparse measurements have substantial scheduler/GC variance. An earlier
matching-only probe found positional and ID keys within 1%, so the dense gains
from avoiding map reconstruction are more persuasive than the small sparse
differences.

## Batch allocation

For the checked-in 1,000-rule/eight-match batch fixture, positional addressing
reduced measured allocation in the prepare-once path from 56.20 KB to 25.80 KB
for 10 events and from 0.54 MB to 0.24 MB for 100 events: about 2.18-2.25x lower.
Wall-clock results varied between runs, so they are not used to claim an
additional positional batch speedup.

## Snapshot representation

Using the component model documented by the parent hardening note, removing the
ID index and entry IDs reduced the already-compact modeled snapshot by another
29-39%:

| Rules / shape | ID-keyed parent | Positional child | Reduction |
| --- | ---: | ---: | ---: |
| 100 / 8 | 6,250 B | 4,207 B | 32.7% |
| 100 / 1 | 6,936 B | 4,685 B | 32.5% |
| 100 / all | 5,757 B | 3,519 B | 38.9% |
| 1,000 / 8 | 65,572 B | 45,805 B | 30.1% |
| 1,000 / 1 | 71,333 B | 50,581 B | 29.1% |
| 1,000 / all | 58,385 B | 37,588 B | 35.6% |

This is a representation model, not resident-process memory. It excludes
Cachex/ETS table overhead and allocator fragmentation equally from both sides.

## Commands

```sh
MIX_ENV=test ../bin/x mix run test/profiling/source_routing_bench.exs
MIX_ENV=test ../bin/x mix run test/profiling/source_routing_batch_bench.exs
MIX_ENV=test ../bin/x mix run /tmp/routing-base-footprint.exs
```
