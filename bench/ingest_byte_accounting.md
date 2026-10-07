# Values-only ingestion accounting (O11Y-2541)

Ingestion accounting excludes field names and structural overhead. Strings use
byte length, integers decimal text length, floats `Float.to_string/1` length,
booleans one byte, and nil zero bytes. This is an accounting policy, not a
ClickHouse storage or transport measurement.

The existing batch estimate remains `:erlang.external_size(event.body)`. Normal
ingestion accumulates accounting during the existing cleaning/key-normalization
pass, adjusts it for generated fields and duplicate messages, and caches both
measurements after source transformations and custom-message generation, before
backend fanout. Queue insertion, retries, S3 batching and ingestion telemetry
reuse the applicable cached measurement.

Body changes must invalidate the caches via `LogEvent.replace_body/2` or update
them explicitly. Custom messages update accounting by a value delta; copy,
enrichment, drop-field and Datadog body changes invalidate the caches. Spool
records intentionally do not persist counters. Reconstructed/uncached events and
normalized-key collisions have a correct full-traversal fallback.

## Reproduce

From a provisioned workspace:

```sh
jj file show -r 2dc251d0 lib/logflare/logs/ingest_transformers.ex > /tmp/byte-accounting-baseline.ex
BYTE_ACCOUNTING_BASELINE=/tmp/byte-accounting-baseline.ex \
  ../bin/x mix run --no-start bench/ingest_byte_accounting.exs
```

The script compiles the pre-feedback transformer under a separate module name,
checks output equivalence, and compares existing sizing, a separate accounting
pass, fused accounting, and three-queue reuse. It measures transformation and
sizing only: not complete ingestion, ETS copying, network calls or compression.

## Local results

Recorded 2026-10-07 on Linux, Elixir 1.19.5/OTP 27, Benchee 1.5.0, one benchmark
worker, two seconds timing plus one second each of warmup, memory and reductions.
Inputs are synthetic; the histogram has 1,024 integers and 1,024 floats. Timing is
noisy, especially for sub-microsecond small logs; these are directional results,
not production throughput guarantees.

Median transformation plus one batch estimate:

| Input | Existing (no values-only accounting) | Separate accounting | Fused accounting |
| --- | ---: | ---: | ---: |
| Small log | 0.208 µs | 0.250 µs | 0.209 µs |
| Key-heavy edge log | 10.21 µs | 10.50 µs | 10.17 µs |
| Nested log | 5.38 µs | 6.00 µs | 5.63 µs |
| Numeric histogram | 13.13 µs | 47.58 µs | 40.71 µs |

For the histogram, fused accounting used 81.45 KiB of allocated memory versus
53.04 KiB without accounting and 77.16 KiB with a separate accounting pass. A
separate scalar-argument list accumulator and integer digit counting without
temporary strings reduced the first fused prototype from 160.67 KiB to 81.45 KiB. Float
text sizing still allocates; fusion does not make numeric accounting free.

Three-queue histogram sizing took a median 116.42 µs when recalculating both
measurements per queue versus 40.79 µs when fusing accounting and caching both
measurements once. The equivalent key-heavy-log medians were 12.71 µs and
10.33 µs. Those comparisons illustrate avoided recomputation, not the overall
performance delta against today's complete ingestion path.
