# Benchmark the Schema helper hot paths without involving the GenServer.
#
# Run with:
#   mix run bench/schema_helpers_bench.exs
#
# Optional env vars:
#   SCHEMA_BENCH_SUITE=all|payload_helpers|builder|schema_helpers
#   BENCH_TIME=5
#   BENCH_WARMUP=2
#   BENCH_MEMORY_TIME=3
#   BENCH_REDUCTION_TIME=3
#
# Public helper functions are exposed via `Logflare.Profiling.SchemaHelpersBench`
# so they can be targeted directly with `:eprof` after this.

Code.require_file("support/schema_helpers_bench.ex", __DIR__)

alias Logflare.Profiling.SchemaHelpersBench

SchemaHelpersBench.run()
