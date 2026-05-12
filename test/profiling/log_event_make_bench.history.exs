[
  %{
    meta: %{
      captured_at: "2026-05-12T15:35:11Z",
      git_sha: "9848addea",
      label: "origin/main baseline",
      machine: "Apple M5"
    },
    results: %{
      "edge log" => %{
        ips: 3250,
        memory_avg_bytes: 1_352_663,
        reductions_avg: 137_370,
        wall_avg_us: 307.45,
        wall_median_us: 298.71,
        wall_p99_us: 451.61
      },
      "otel trace" => %{
        ips: 12_600,
        memory_avg_bytes: 304_087,
        reductions_avg: 31_230,
        wall_avg_us: 79.35,
        wall_median_us: 78.0,
        wall_p99_us: 103.83
      }
    }
  }
]
