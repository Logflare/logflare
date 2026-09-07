# Usage: mix run --no-start bench/compare_encode_e2e.exs
#
# Loads the saved before/after Benchee results produced by
# bench/clickhouse_encode_e2e.exs and prints a per-job comparison
# (after is shown relative to before, the slowest tag).

save_dir = System.get_env("BENCH_SAVE_DIR", "/tmp")

Benchee.run(
  %{},
  load: Path.join(save_dir, "encode_e2e_*.benchee"),
  print: [configuration: false]
)
