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

The candidate stores write-concurrent `:counters` references in read-concurrent
ETS lookup tables. Three matching fresh-VM trials of both workloads reported no
database-lock conflicts. An all-category follow-up reported only scheduler
run-queue and task process locks, with no counter lock replacing the removed ETS
contention.

Median instrumented-VM elapsed time also dropped from 0.099784 seconds to
0.001618 seconds for the source scenario and from 0.099877 seconds to 0.001602
seconds for the system scenario. These timings describe the lock-counting
emulator; the collision counts, rather than absolute throughput, are the primary
result.

Concurrent reads of the ingest queue mapper were also profiled. They produced
no lock conflicts, so its ETS configuration was left unchanged.
