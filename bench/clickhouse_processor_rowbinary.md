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

The phase comparison uses `BATCH_SIZE=60000`, `BATCHES=3`, `WARMUP_BATCHES=1`,
`CONCURRENCY=6`, and `PROCESSOR_CHUNK_SIZE=1000` for each event type and the `baseline`,
`processor`, `encode`, and `compress` scenarios. The concurrency sweep holds those
settings constant while testing `CONCURRENCY=4,6,8,12`. Memory runs use
`BATCH_SIZE=60000` and compare `SCENARIO=event` with `SCENARIO=encoded` in separate OS
processes under `/usr/bin/time -v`.

## Processor concurrency

The processor scenario uses Broadway-shaped chunks of at most 1,000 rows. Results are
rows per wall-clock second; each result covers three measured 60,000-row batches after
a warmup batch.

| Event type | 4 processors | 6 processors | 8 processors | 12 processors |
| --- | ---: | ---: | ---: | ---: |
| log | 158,534 | 170,951 | 171,329 | **174,227** |
| metric | 99,290 | **107,397** | 106,749 | 105,733 |
| trace | 120,384 | 135,506 | 136,458 | **138,754** |

Six processors remain the balanced setting on six schedulers. Twelve processors gain
only about 2% for logs and traces while losing about 2% for metrics, at the cost of
additional scheduler competition with gzip. The existing `max_demand: 1_000` also
remains suitable: six-processor encoding plus gzip completes 60,000 rows in roughly
0.35–0.58 seconds, comfortably below the 5-second batch timeout.

## End-to-end comparison with current main

Current `main` at `ee61fab1` was measured in a separate clean workspace on the same
host using the same fixtures and fixed-work settings. Its production-equivalent path is
ETS lookup, `Mapper.map/3`, Elixir RowBinary encoding, and streaming gzip. Compressed
output byte counts matched both stacked scenarios exactly.

| Event type | Current main | Fused batch-side (#3759) | Processor-side (#3837) | #3837 elapsed change vs main |
| --- | ---: | ---: | ---: | ---: |
| log | 75,520 rows/s | 78,693 rows/s | 168,077 rows/s | -55% |
| metric | 37,309 rows/s | 49,452 rows/s | 102,807 rows/s | -64% |
| trace | 38,807 rows/s | 66,893 rows/s | 137,989 rows/s | -72% |

The combined stack therefore shortens the measured local path by 55–72% versus current
main. The next section isolates #3837's incremental effect on top of the fused mapper.

## Incremental batch critical path

| Event type | Fused batch-side (#3759) | Processor-side (#3837) | Gzip only |
| --- | ---: | ---: | ---: |
| log | 78,693 rows/s | 168,077 rows/s | 230,489 rows/s |
| metric | 49,452 rows/s | 102,807 rows/s | 178,773 rows/s |
| trace | 66,893 rows/s | 137,989 rows/s | 219,139 rows/s |

Processor-side encoding shortens the local 60,000-row encode/compress critical path by
roughly 51–53%. Encoding-only throughput with six processors is 379k log, 230k metric,
and 334k trace rows/s; gzip-only throughput shows that serial compression becomes the
remaining local bottleneck. The change does not remove CPU work—it overlaps and
parallelizes fused encoding before the batch trigger.

Batch-processor concurrency is reduced from 32 to 4. The local benchmark shows that one
gzip worker can process roughly 179k–230k rows/s, so additional workers primarily cover
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
| log | 328.2 MB | 229.3 MB | -30.1% |
| metric | 472.4 MB | 275.0 MB | -41.8% |
| trace | 402.8 MB | 249.6 MB | -38.0% |

Maximum RSS changed by at most 1.6% because the benchmark includes the bounded transient
handoff where an event copy and its new binary coexist. After handoff, ETS memory falls
sharply while binary memory rises; total VM memory is lower.

Batch-processor concurrency is decoupled from the count-based in-flight ceiling. Four
workers bound concurrent gzip/HTTP inserts, while the producer retains the previous
64-batch capacity of 3.84 million rows per backend. During downstream stalls, encoded
partial and completed batches can accumulate in Broadway's `:ch` batcher up to that
cap; additional backlog remains in `IngestEventQueue`. The lower steady-state memory
above supports retaining this capacity while byte-based control remains useful future
hardening.
