# ClickHouse processor-side RowBinary benchmark

This benchmark compares the fused-mapper baseline, where each batch processor
resolves and encodes every row serially after a batch triggers, with processor-side
encoding followed by batch-only gzip work.

## Environment

- Linux aarch64
- 6 online BEAM schedulers
- production-shaped synthetic log, metric, and trace events
- 60,000-row batches
- fused mapper output validated byte-for-byte against the baseline

Commands use `MIX_ENV=test mix run --no-start` with:

- `bench/clickhouse_processor_rowbinary.exs`
- `bench/clickhouse_processor_rowbinary_memory.exs`

## Processor concurrency

The processor scenario uses Broadway-shaped chunks of at most 1,000 rows. Results are
rows per wall-clock second; each result covers two measured 60,000-row batches after a
warmup batch.

| Event type | 4 processors | 6 processors | 8 processors | 12 processors |
| --- | ---: | ---: | ---: | ---: |
| log | 160,738 | **177,639** | 172,985 | 172,124 |
| metric | 102,026 | 109,377 | **110,139** | 106,113 |
| trace | 122,252 | 137,700 | 136,841 | **140,546** |

Six processors are the balanced setting on six schedulers. Higher concurrency has no
consistent benefit across event types and increases scheduler competition with gzip.
The existing `max_demand: 1_000` also remains suitable: serial fused encoding measured
about 9–19 ms per 1,000 production-shaped rows, comfortably below the 5-second batch
timeout.

## Batch critical path

| Event type | Batch-side encode + gzip | Processor encode, then gzip | Gzip only |
| --- | ---: | ---: | ---: |
| log | 63,330 rows/s | 177,639 rows/s | 200,831 rows/s |
| metric | 51,659 rows/s | 109,377 rows/s | 179,262 rows/s |
| trace | 69,281 rows/s | 137,700 rows/s | 218,439 rows/s |

Processor-side encoding shortens the local 60,000-row encode/compress critical path by
roughly 50–64%. It does not remove CPU work; it overlaps and parallelizes fused encoding
before the batch trigger. Gzip remains serial per batch.

Batch-processor concurrency is reduced from 32 to 4. The local benchmark shows that one
gzip worker can process roughly 179k–218k rows/s, so additional workers primarily cover
HTTP latency rather than CPU throughput. Production insert timeouts provide the missing
downstream evidence: 32 workers let one backend issue 32 simultaneous HTTP/1 inserts,
occupy most of the shared primary Finch pool, exceed the async pool's connection count,
and amplify retries when ClickHouse slows. Four workers retain bounded HTTP overlap
without allowing one backend to monopolize the connection pools.

## Memory

Steady-state VM memory was measured after garbage collection while holding either:

1. claimed pointers plus full `LogEvent`s in the generation store, or
2. processor messages plus `EncodedRow`s replacing those generation values.

The ETS value and Broadway message share the same reference-counted RowBinary payload.

| Event type | Full-event state | Encoded-row state | Change |
| --- | ---: | ---: | ---: |
| log | 328.9 MB | 230.7 MB | -29.9% |
| metric | 471.6 MB | 275.0 MB | -41.7% |
| trace | 403.4 MB | 249.6 MB | -38.1% |

Maximum RSS changed by less than 1% because the benchmark includes the bounded transient
handoff where an event copy and its new binary coexist. After handoff, ETS memory falls
sharply while binary memory rises; total VM memory is lower.

Reducing batch-processor concurrency also lowers the count-based in-flight ceiling from
3.84 million to 480,000 rows per backend. Together with the lower steady-state memory
above, this supports deferring byte-based control while retaining it as useful future
hardening.
