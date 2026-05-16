[
  %{
    meta: %{
      captured_at: "2026-05-16T21:38:16.498582Z",
      git_sha: "9760d277d",
      label: "o11y-1828 fused column-spec walk",
      machine: nil
    },
    results: %{
      "edge log" => %{
        ips: 35592,
        memory_avg_bytes: 83152,
        reductions_avg: 5603,
        wall_avg_us: 28.1,
        wall_median_us: 25.71,
        wall_p99_us: 49.88
      },
      "edge log + all" => %{
        ips: 34084,
        memory_avg_bytes: 79872,
        reductions_avg: 5415,
        wall_avg_us: 29.34,
        wall_median_us: 28.29,
        wall_p99_us: 44.65
      },
      "edge log + copy" => %{
        ips: 33606,
        memory_avg_bytes: 83296,
        reductions_avg: 5718,
        wall_avg_us: 29.76,
        wall_median_us: 27.88,
        wall_p99_us: 45.08
      },
      "edge log + copy + kv" => %{
        ips: 31828,
        memory_avg_bytes: 86120,
        reductions_avg: 5717,
        wall_avg_us: 31.42,
        wall_median_us: 29.54,
        wall_p99_us: 47.29
      },
      "edge log + drop" => %{
        ips: 34399,
        memory_avg_bytes: 74336,
        reductions_avg: 5155,
        wall_avg_us: 29.07,
        wall_median_us: 27.25,
        wall_p99_us: 44.29
      },
      "edge log + kv" => %{
        ips: 32868,
        memory_avg_bytes: 83344,
        reductions_avg: 5517,
        wall_avg_us: 30.42,
        wall_median_us: 28.67,
        wall_p99_us: 46.17
      },
      "otel trace" => %{
        ips: 90139,
        memory_avg_bytes: 31374,
        reductions_avg: 2506,
        wall_avg_us: 11.09,
        wall_median_us: 9.92,
        wall_p99_us: 34.04
      },
      "otel trace + all" => %{
        ips: 79886,
        memory_avg_bytes: 35110,
        reductions_avg: 2661,
        wall_avg_us: 12.52,
        wall_median_us: 11.38,
        wall_p99_us: 34.04
      },
      "otel trace + copy" => %{
        ips: 89031,
        memory_avg_bytes: 32782,
        reductions_avg: 2546,
        wall_avg_us: 11.23,
        wall_median_us: 10.0,
        wall_p99_us: 36.75
      },
      "otel trace + copy + kv" => %{
        ips: 81271,
        memory_avg_bytes: 34462,
        reductions_avg: 2717,
        wall_avg_us: 12.3,
        wall_median_us: 11.0,
        wall_p99_us: 35.96
      },
      "otel trace + drop" => %{
        ips: 92791,
        memory_avg_bytes: 31728,
        reductions_avg: 2438,
        wall_avg_us: 10.78,
        wall_median_us: 9.58,
        wall_p99_us: 35.42
      },
      "otel trace + kv" => %{
        ips: 80978,
        memory_avg_bytes: 33830,
        reductions_avg: 2635,
        wall_avg_us: 12.35,
        wall_median_us: 10.71,
        wall_p99_us: 34.96
      }
    }
  },
  %{
    meta: %{
      captured_at: "2026-05-16T20:44:01.594108Z",
      git_sha: "640151ae9",
      label: "o11y-1829 single-pass validate",
      machine: nil
    },
    results: %{
      "edge log" => %{
        ips: 17141,
        memory_avg_bytes: 83808,
        reductions_avg: 5620,
        wall_avg_us: 58.34,
        wall_median_us: 57.75,
        wall_p99_us: 67.63
      },
      "edge log + all" => %{
        ips: 16109,
        memory_avg_bytes: 81064,
        reductions_avg: 5677,
        wall_avg_us: 62.08,
        wall_median_us: 61.79,
        wall_p99_us: 71.25
      },
      "edge log + copy" => %{
        ips: 16528,
        memory_avg_bytes: 84112,
        reductions_avg: 5754,
        wall_avg_us: 60.5,
        wall_median_us: 60.33,
        wall_p99_us: 70.83
      },
      "edge log + copy + kv" => %{
        ips: 15846,
        memory_avg_bytes: 86544,
        reductions_avg: 5913,
        wall_avg_us: 63.11,
        wall_median_us: 62.75,
        wall_p99_us: 72.97
      },
      "edge log + drop" => %{
        ips: 16859,
        memory_avg_bytes: 74896,
        reductions_avg: 5245,
        wall_avg_us: 59.32,
        wall_median_us: 59.08,
        wall_p99_us: 68.63
      },
      "edge log + kv" => %{
        ips: 16209,
        memory_avg_bytes: 84048,
        reductions_avg: 5679,
        wall_avg_us: 61.69,
        wall_median_us: 61.5,
        wall_p99_us: 70.54
      },
      "otel trace" => %{
        ips: 40608,
        memory_avg_bytes: 31926,
        reductions_avg: 2432,
        wall_avg_us: 24.63,
        wall_median_us: 24.13,
        wall_p99_us: 30.92
      },
      "otel trace + all" => %{
        ips: 37229,
        memory_avg_bytes: 35726,
        reductions_avg: 2664,
        wall_avg_us: 26.86,
        wall_median_us: 26.17,
        wall_p99_us: 32.88
      },
      "otel trace + copy" => %{
        ips: 38782,
        memory_avg_bytes: 32862,
        reductions_avg: 2584,
        wall_avg_us: 25.79,
        wall_median_us: 25.29,
        wall_p99_us: 31.71
      },
      "otel trace + copy + kv" => %{
        ips: 38117,
        memory_avg_bytes: 34709,
        reductions_avg: 2749,
        wall_avg_us: 26.24,
        wall_median_us: 25.88,
        wall_p99_us: 32.08
      },
      "otel trace + drop" => %{
        ips: 40186,
        memory_avg_bytes: 32030,
        reductions_avg: 2435,
        wall_avg_us: 24.88,
        wall_median_us: 24.67,
        wall_p99_us: 29.92
      },
      "otel trace + kv" => %{
        ips: 38859,
        memory_avg_bytes: 33938,
        reductions_avg: 2591,
        wall_avg_us: 25.73,
        wall_median_us: 25.33,
        wall_p99_us: 32.17
      }
    }
  },
  %{
    meta: %{
      captured_at: "2026-05-16T20:41:11.642611Z",
      git_sha: "8e03624a4",
      label: "o11y-1829 baseline (main)",
      machine: nil
    },
    results: %{
      "edge log" => %{
        ips: 12851,
        memory_avg_bytes: 141968,
        reductions_avg: 9735,
        wall_avg_us: 77.82,
        wall_median_us: 77.25,
        wall_p99_us: 94.79
      },
      "edge log + all" => %{
        ips: 12585,
        memory_avg_bytes: 130088,
        reductions_avg: 9100,
        wall_avg_us: 79.46,
        wall_median_us: 79.54,
        wall_p99_us: 93.92
      },
      "edge log + copy" => %{
        ips: 12499,
        memory_avg_bytes: 143992,
        reductions_avg: 10147,
        wall_avg_us: 80.01,
        wall_median_us: 79.38,
        wall_p99_us: 98.67
      },
      "edge log + copy + kv" => %{
        ips: 11656,
        memory_avg_bytes: 146496,
        reductions_avg: 10314,
        wall_avg_us: 85.79,
        wall_median_us: 85.46,
        wall_p99_us: 103.5
      },
      "edge log + drop" => %{
        ips: 13271,
        memory_avg_bytes: 121560,
        reductions_avg: 8580,
        wall_avg_us: 75.35,
        wall_median_us: 74.58,
        wall_p99_us: 88.88
      },
      "edge log + kv" => %{
        ips: 12056,
        memory_avg_bytes: 141192,
        reductions_avg: 9799,
        wall_avg_us: 82.95,
        wall_median_us: 82.21,
        wall_p99_us: 101.1
      },
      "otel trace" => %{
        ips: 26663,
        memory_avg_bytes: 57782,
        reductions_avg: 4307,
        wall_avg_us: 37.5,
        wall_median_us: 37.54,
        wall_p99_us: 47.79
      },
      "otel trace + all" => %{
        ips: 25396,
        memory_avg_bytes: 59790,
        reductions_avg: 4436,
        wall_avg_us: 39.38,
        wall_median_us: 38.83,
        wall_p99_us: 46.96
      },
      "otel trace + copy" => %{
        ips: 25335,
        memory_avg_bytes: 60426,
        reductions_avg: 4632,
        wall_avg_us: 39.47,
        wall_median_us: 39.42,
        wall_p99_us: 47.04
      },
      "otel trace + copy + kv" => %{
        ips: 24708,
        memory_avg_bytes: 63742,
        reductions_avg: 4788,
        wall_avg_us: 40.47,
        wall_median_us: 40.54,
        wall_p99_us: 48.71
      },
      "otel trace + drop" => %{
        ips: 27713,
        memory_avg_bytes: 51050,
        reductions_avg: 3956,
        wall_avg_us: 36.08,
        wall_median_us: 35.71,
        wall_p99_us: 42.33
      },
      "otel trace + kv" => %{
        ips: 24902,
        memory_avg_bytes: 60350,
        reductions_avg: 4562,
        wall_avg_us: 40.16,
        wall_median_us: 39.5,
        wall_p99_us: 47.38
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
        ips: 12510,
        memory_avg_bytes: 149268,
        reductions_avg: 10430,
        wall_avg_us: 79.93,
        wall_median_us: 78.96,
        wall_p99_us: 100.88
      },
      "otel trace" => %{
        ips: 26190,
        memory_avg_bytes: 59893,
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
        memory_avg_bytes: 1352663,
        reductions_avg: 137370,
        wall_avg_us: 307.45,
        wall_median_us: 298.71,
        wall_p99_us: 451.61
      },
      "otel trace" => %{
        ips: 12600,
        memory_avg_bytes: 304087,
        reductions_avg: 31230,
        wall_avg_us: 79.35,
        wall_median_us: 78.0,
        wall_p99_us: 103.83
      }
    }
  }
]
