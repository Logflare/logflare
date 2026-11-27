# Ingest counter contention

`Logflare.Backends.ingest_logs/4` increments one source counter and one
node-wide counter **per call**, not per event. This PR keeps the existing ETS
counter operations and enables `write_concurrency: :auto` on both tables.
There are no new counter references or reset semantics.

## Lock-counting benchmark

`ingest_counter_lcnt.exs` exercises the actual counter modules. To build an
OTP 27 lock-counting VM, configure OTP with `--enable-lock-counter`, then run
`make && make install`. With that installation available, run each scenario
in a fresh VM from this workspace:

```sh
../bin/x sh -c 'PATH=/path/to/lock-counting-otp/bin:$PATH \
  ERL_FLAGS="-emu_type lcnt +S 8:8" SCENARIO=source WORKERS=8 ITERATIONS=1000 \
  mix run --no-start bench/ingest_counter_lcnt.exs'
# Repeat with SCENARIO=system for the node-wide counter.
```

The script rejects a normal VM, starts all workers behind one barrier, checks
the exact final count, and reports database-lock conflicts. This is an
**intentionally saturated** same-key workload, not a realistic batch rate.

On OTP 27.3.4.6, the old tables without write concurrency had median
7,999/8,000 source-table and 7,998/8,000 system-table lock collisions in
three trials. With `:auto`, three matching trials still had database-lock
conflicts on table and/or hash-slot locks, but the original single-table
lock bottleneck was reduced. The number and location of internal lock
acquisitions change under `:auto`, so collision ratios across lock types are
not directly comparable. The setting does **not** make updates lock-free.

## Normal-VM throughput

Run `ERL_FLAGS='+S 8:8' ../bin/x mix run --no-start
bench/ingest_counter_throughput.exs` to compare the original source-counter
update API with the current `:auto` API. Three trials on the same OTP 27
runner yielded these medians:

| Workers continuously updating one source | Original updates/s | `:auto` updates/s |
| ---: | ---: | ---: |
| 1 | 35.5M | 28.7M |
| 2 | 4.15M | 2.87M |
| 8 | 81.8K | 293.9K |

The crossover depends on concurrency and burst shape, not simply the average
event rate. In a separate paced, paired-counter probe (eight workers, 500
events counted per call), the original ETS path and `:auto` sustained the same
~95, ~800, and ~2,660 calls/s. The requested 4,000/s run only achieved
~2,660/s because millisecond sleeps limited the driver. At ~95 calls/s, the
old source table saw 2–19 collisions per 296 calls across three lock-counting
trials, with generally microseconds of total wait; synchronized bursts at
similar average rate caused more collisions. `:auto` usually reduced the wait.

These are isolated counter-path experiments. They do not measure end-to-end
ingest throughput or establish the batch-call rate of a production node. The
previously profiled ingest queue mapper showed no lock conflicts and remains
unchanged.
