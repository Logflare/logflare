[
  %{
    meta: %{
      captured_at: "2026-05-15T23:42:03.984586Z",
      git_sha: "a71618902",
      label: "o11y-1825 binary keys with UTF-8 normalization",
      machine: nil
    },
    results: %{
      "edge log" => %{
        ips: 12_551,
        memory_avg_bytes: 139_392,
        reductions_avg: 9008,
        wall_avg_us: 79.67,
        wall_median_us: 78.46,
        wall_p99_us: 95.92
      },
      "edge log + all" => %{
        ips: 12_077,
        memory_avg_bytes: 128_672,
        reductions_avg: 8352,
        wall_avg_us: 82.8,
        wall_median_us: 82.17,
        wall_p99_us: 95.73
      },
      "edge log + copy" => %{
        ips: 11_941,
        memory_avg_bytes: 141_600,
        reductions_avg: 9336,
        wall_avg_us: 83.75,
        wall_median_us: 82.88,
        wall_p99_us: 103.0
      },
      "edge log + copy + kv" => %{
        ips: 11_484,
        memory_avg_bytes: 145_232,
        reductions_avg: 9420,
        wall_avg_us: 87.08,
        wall_median_us: 86.46,
        wall_p99_us: 104.58
      },
      "edge log + drop" => %{
        ips: 12_880,
        memory_avg_bytes: 118_816,
        reductions_avg: 7955,
        wall_avg_us: 77.64,
        wall_median_us: 76.58,
        wall_p99_us: 91.57
      },
      "edge log + kv" => %{
        ips: 11_667,
        memory_avg_bytes: 138_360,
        reductions_avg: 9004,
        wall_avg_us: 85.71,
        wall_median_us: 84.96,
        wall_p99_us: 101.54
      },
      "otel trace" => %{
        ips: 25_336,
        memory_avg_bytes: 55_926,
        reductions_avg: 4005,
        wall_avg_us: 39.47,
        wall_median_us: 39.21,
        wall_p99_us: 47.04
      },
      "otel trace + all" => %{
        ips: 24_958,
        memory_avg_bytes: 58_678,
        reductions_avg: 4126,
        wall_avg_us: 40.07,
        wall_median_us: 39.38,
        wall_p99_us: 48.42
      },
      "otel trace + copy" => %{
        ips: 24_312,
        memory_avg_bytes: 58_798,
        reductions_avg: 4283,
        wall_avg_us: 41.13,
        wall_median_us: 40.42,
        wall_p99_us: 47.63
      },
      "otel trace + copy + kv" => %{
        ips: 23_864,
        memory_avg_bytes: 61_734,
        reductions_avg: 4412,
        wall_avg_us: 41.9,
        wall_median_us: 41.0,
        wall_p99_us: 50.08
      },
      "otel trace + drop" => %{
        ips: 27_556,
        memory_avg_bytes: 50_640,
        reductions_avg: 3749,
        wall_avg_us: 36.29,
        wall_median_us: 36.04,
        wall_p99_us: 43.38
      },
      "otel trace + kv" => %{
        ips: 24_652,
        memory_avg_bytes: 57_766,
        reductions_avg: 4216,
        wall_avg_us: 40.57,
        wall_median_us: 40.04,
        wall_p99_us: 48.13
      }
    }
  },
  %{
    meta: %{
      captured_at: "2026-05-15T23:39:04.149731Z",
      git_sha: "8e03624a4",
      label: "o11y-1825 baseline (main)",
      machine: nil
    },
    results: %{
      "edge log" => %{
        ips: 12_984,
        memory_avg_bytes: 141_712,
        reductions_avg: 9777,
        wall_avg_us: 77.02,
        wall_median_us: 76.21,
        wall_p99_us: 95.71
      },
      "edge log + all" => %{
        ips: 12_636,
        memory_avg_bytes: 131_104,
        reductions_avg: 9101,
        wall_avg_us: 79.14,
        wall_median_us: 79.29,
        wall_p99_us: 93.67
      },
      "edge log + copy" => %{
        ips: 12_526,
        memory_avg_bytes: 143_992,
        reductions_avg: 10_147,
        wall_avg_us: 79.83,
        wall_median_us: 79.38,
        wall_p99_us: 96.83
      },
      "edge log + copy + kv" => %{
        ips: 11_993,
        memory_avg_bytes: 148_056,
        reductions_avg: 10_301,
        wall_avg_us: 83.38,
        wall_median_us: 82.58,
        wall_p99_us: 101.46
      },
      "edge log + drop" => %{
        ips: 13_444,
        memory_avg_bytes: 121_560,
        reductions_avg: 8579,
        wall_avg_us: 74.38,
        wall_median_us: 73.5,
        wall_p99_us: 88.33
      },
      "edge log + kv" => %{
        ips: 12_199,
        memory_avg_bytes: 141_648,
        reductions_avg: 9969,
        wall_avg_us: 81.97,
        wall_median_us: 81.21,
        wall_p99_us: 98.29
      },
      "otel trace" => %{
        ips: 25_913,
        memory_avg_bytes: 57_910,
        reductions_avg: 4313,
        wall_avg_us: 38.59,
        wall_median_us: 38.46,
        wall_p99_us: 45.25
      },
      "otel trace + all" => %{
        ips: 25_638,
        memory_avg_bytes: 59_790,
        reductions_avg: 4436,
        wall_avg_us: 39.0,
        wall_median_us: 38.58,
        wall_p99_us: 45.63
      },
      "otel trace + copy" => %{
        ips: 25_000,
        memory_avg_bytes: 60_414,
        reductions_avg: 4603,
        wall_avg_us: 40.0,
        wall_median_us: 39.71,
        wall_p99_us: 46.7
      },
      "otel trace + copy + kv" => %{
        ips: 24_397,
        memory_avg_bytes: 62_720,
        reductions_avg: 4798,
        wall_avg_us: 40.99,
        wall_median_us: 40.5,
        wall_p99_us: 49.38
      },
      "otel trace + drop" => %{
        ips: 28_267,
        memory_avg_bytes: 50_974,
        reductions_avg: 3940,
        wall_avg_us: 35.38,
        wall_median_us: 35.17,
        wall_p99_us: 41.42
      },
      "otel trace + kv" => %{
        ips: 24_818,
        memory_avg_bytes: 60_117,
        reductions_avg: 4441,
        wall_avg_us: 40.29,
        wall_median_us: 39.63,
        wall_p99_us: 46.79
      }
    }
  },
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
