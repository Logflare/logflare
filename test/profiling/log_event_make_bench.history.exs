[
  %{
    meta: %{
      captured_at: "2026-05-12T15:45:32Z",
      git_sha: "4bf5d09fb",
      label: "post-flatten_typemap rewrite",
      machine: "Apple M5"
    },
    results: %{
      "edge log" => %{
        ips: 12_510,
        memory_avg_bytes: 149_268,
        reductions_avg: 10_430,
        wall_avg_us: 79.93,
        wall_median_us: 78.96,
        wall_p99_us: 100.88
      },
      "otel trace" => %{
        ips: 26_190,
        memory_avg_bytes: 59_893,
        reductions_avg: 4630,
        wall_avg_us: 38.18,
        wall_median_us: 38.38,
        wall_p99_us: 48.13
      }
    }
  },
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
