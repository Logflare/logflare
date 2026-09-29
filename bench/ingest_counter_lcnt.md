# Ingest counter lock contention

`ingest_counter_lcnt.exs` profiles the two counters updated by every
`Logflare.Backends.ingest_logs/4` call. It uses OTP's built-in `:lcnt` tool and
does not add a project dependency.

## Run

Build OTP with its additional lock-counting emulator, then place that OTP
installation first on `PATH`:

```sh
./configure --prefix=/path/to/lock-counting-otp --enable-lock-counter
make
make install
```

Run each scenario in a fresh VM. The benchmark rejects a normal VM so a missing
`-emu_type lcnt` cannot silently produce invalid results.

```sh
PATH=/path/to/lock-counting-otp/bin:$PATH \
ERL_FLAGS='-emu_type lcnt +S 8:8' \
SCENARIO=source WORKERS=8 ITERATIONS=1000 \
  mix run --no-start bench/ingest_counter_lcnt.exs

PATH=/path/to/lock-counting-otp/bin:$PATH \
ERL_FLAGS='-emu_type lcnt +S 8:8' \
SCENARIO=system WORKERS=8 ITERATIONS=1000 \
  mix run --no-start bench/ingest_counter_lcnt.exs
```

The workload starts all workers behind one barrier, clears the lock counters,
runs a fixed number of increments, verifies the exact final count, and prints
uncombined database-lock conflicts.

## OTP 27.3.4.6 results

The baseline stored each metric tuple directly in ETS and used
`:ets.update_counter/4`. Three fresh-VM trials with eight workers contending on
one source or the global system metric found that nearly every acquisition
collided:

| Scenario | ETS lock | Tries/trial | Median collisions (range) | Median ratio (range) | Median wait time |
| --- | --- | ---: | ---: | ---: | ---: |
| source | `db_tab table_counters` | 8,000 | 7,999 (7,994–7,999) | 99.9875% (99.9250–99.9875%) | 768,797 us |
| system | `db_tab system_counter` | 8,000 | 7,998 (7,986–7,999) | 99.9750% (99.8250–99.9875%) | 768,284 us |

The initial candidate stored write-concurrent `:counters` references in
read-concurrent ETS lookup tables. Three matching fresh-VM trials of both
workloads reported no database-lock conflicts. An all-category follow-up
reported only scheduler run-queue and task process locks, with no counter lock
replacing the removed ETS contention. The source counter was subsequently
changed to fixed-size `:atomics` to bound per-source memory; the global counter
still uses `:counters`.

Three fresh-VM trials of the **final hybrid** on the same OTP 27.3.4.6
lock-counting build, with eight workers × 1,000 increments per scenario, all
reported zero database-lock conflicts and verified the exact final count.
Source elapsed times were 0.002658, 0.002881, and 0.002763 seconds (median
0.002763); system elapsed times were 0.001494, 0.001345, and 0.001305 seconds
(median 0.001345). An all-category trial of each scenario found only scheduler
run-queue and process-message-queue locks, with no replacement counter lock.

For comparison, the initial sharded candidate's median instrumented-VM elapsed
time was 0.001618 seconds for source and 0.001602 seconds for system, versus
0.099784 and 0.099877 seconds for the old ETS baseline. These timings describe
the lock-counting emulator; collision counts, rather than absolute throughput,
are the primary result.

Concurrent reads of the ingest queue mapper were also profiled. They produced
no lock conflicts, so its ETS configuration was left unchanged.

## Normal-VM source-counter comparison

Run `ERL_FLAGS='+S 8:8' ../bin/x mix run --no-start bench/ingest_counter_throughput.exs`
from this workspace. The script uses three trials of one source key per scenario,
verifies the exact final count, and compares primitive-level simulations of the
old ETS update and initial sharded counter with the **actual** current source
counter API (including the post-add reset check). OTP 27 on the local runner:

| Concurrent writers | Old ETS median ops/s | Sharded median ops/s | Hybrid median ops/s |
| ---: | ---: | ---: | ---: |
| 1 | 53.5M | 32.7M | 15.8M |
| 2 | 3.53M | 23.0M | 9.76M |
| 8 | 81.5K | 13.2M | 5.09M |

These isolated, continuously contended updates are not end-to-end ingest
throughput. The extra ref verification has an uncontended per-call cost, while
the hybrid remains much faster than table-locked ETS under sustained contention.

A separate 1,000-source allocation probe (ETS storage plus counter refs) on the
same OTP 27 runner measured 117 KB for old ETS, 733 KB for sharded counters at
8 schedulers (4.32 MB at 64), and 189 KB for the hybrid at both scheduler
counts. A six-slot `:atomics` ref is 88 bytes; a six-slot write-concurrent
`:counters` ref is 608 bytes at 8 schedulers and 4,192 bytes at 64. These are
allocation estimates, not a whole-application memory benchmark.
