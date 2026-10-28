# Broadway processor-message metric sampling

This benchmark measures the global `broadway.processor.message.stop.duration`
metric outside the S3 spool producer workload. It uses the production
`Logflare.Telemetry.metrics/0` definitions, the real `OtelMetricExporter`
telemetry handlers and metric store, and a fresh BEAM instance for every
sampling condition.

## Scenarios

- `noop` runs an identity `handle_message/3` callback with a gated in-memory
  producer and acknowledger. It deliberately minimizes application work and is
  an upper bound on the *relative* cost of attaching the per-message handler.
- `clickhouse` runs the real `BufferProducer`, `IngestEventQueue` pointer and
  in-flight lifecycle, ClickHouse `Pipeline.transform/2`,
  `Pipeline.handle_message/3`, mapper/RowBinary encoding, gzip batching, and
  `Pipeline.ack/3`. The network-facing batch callback is replaced with
  benchmark-local gzip and an in-memory sink. Log, metric, and trace event
  shapes are supported.

Both scenarios retain the other three production Broadway metrics. The runner
compares `disabled`, `10000`, `1000`, `100`, `10`, and `1` in repeated mirrored
order. The Python summary validates configuration parity, complete fixed work,
handler identity, observed sample counts, output batch counts, and the mirrored
execution order.

## Reproduction

```bash
# Cheapest possible processor, one processor worker
bench/run_broadway_metric_sampling.sh noop

# Cheap processor with production-comparable concurrency
PROCESSOR_CONCURRENCY=6 \
bench/run_broadway_metric_sampling.sh noop

# ClickHouse processor path; EVENT_TYPE can be log, metric, or trace
EVENT_TYPE=log \
bench/run_broadway_metric_sampling.sh clickhouse
```

The runner accepts `EVENTS`, `WARMUPS`, `TRIALS`, `CYCLES`, `SCHEDULERS`,
`PROCESSOR_CONCURRENCY`, `BATCHER_CONCURRENCY`, `BATCH_SIZE`,
`BATCH_TIMEOUT_MS`, `PAYLOAD_SHAPE`, and `RESULT_DIR` overrides. `EVENTS` must
be divisible by `BATCH_SIZE` so timed trials contain only complete batches.

## Results

The full runs below used six schedulers, two warmups and five measured trials
per block, and two mirrored cycles. This produced 20 measured samples per
condition. No-op blocks processed 100,000 events per trial; ClickHouse blocks
processed 10,000 realistic events per trial. The predefined decision budget was
at most 5% median throughput loss and at most 5% median reductions/event growth
relative to omitting the per-message metric handler.

| Scenario | Processor concurrency | Disabled baseline events/s | 1% throughput delta | Disabled reductions/event | 1% reductions/event delta | Budget |
|---|---:|---:|---:|---:|---:|:---:|
| No-op identity | 1 | 1,127,750 | -13.14% | 67.44 | +18.76% | upper bound; fail |
| No-op identity | 6 | 1,875,530 | -14.05% | 67.77 | +18.54% | upper bound; fail |
| ClickHouse log | 6 | 110,270 | -2.46% | 901.96 | +2.34% | pass |
| ClickHouse metric | 6 | 70,388 | -2.92% | 1,210.34 | +1.31% | pass |
| ClickHouse trace | 6 | 88,222 | +1.48% | 1,010.14 | +2.66% | pass |

The no-op scenario also showed approximately 16.4–16.7% reductions/event growth
at a 0.01% sample rate. This confirms that the dominant cost for a nearly empty
processor is attaching and evaluating the synchronous handler predicate, not
recording the retained 1% of observations. That result is an intentional worst
case rather than a representative production throughput claim.

All three ClickHouse event shapes remained within the predefined budget at the
1% default. Combined with the S3 spool result, this shows that representative
CPU-bearing production processor paths absorb the fixed predicate cost without
the large regression seen when every observation is retained. The no-op result
also documents the risk for future extremely cheap, high-volume Broadway
processors.

## Limitations

- The ClickHouse scenario replaces the production network-facing batch callback,
  excluding its HTTP/server latency and ancillary telemetry while retaining
  local mapping, RowBinary, gzip, batching, queue, and acknowledgement work.
- The no-op scenario has almost no application work and therefore magnifies any
  fixed per-message cost.
- Payloads are deterministic synthetic fixtures.
- Absolute throughput is machine-specific. Same-environment deltas and
  reductions/event are the relevant comparison.
- Exporter generation rotation and network metric export are excluded.
