[
  %{
    meta: %{
      captured_at: "2026-05-28T14:16:03.376001Z",
      git_sha: "1a214bf4b",
      label: "baseline pre-O11Y-1892",
      machine: "Darwin Bartoszs-MacBook-Pro.local arm64"
    },
    results: %{
      "rt drain 100 project= match | edge" => %{
        ips: 853_075,
        memory_avg_bytes: 384,
        reductions_avg: 734,
        wall_avg_us: 1.17,
        wall_median_us: 1.17,
        wall_p99_us: 1.5
      },
      "rt drain 100 project= match | otel" => %{
        ips: 688_607,
        memory_avg_bytes: 472,
        reductions_avg: 755,
        wall_avg_us: 1.45,
        wall_median_us: 1.42,
        wall_p99_us: 1.88
      },
      "rt drain 100 project= miss | edge" => %{
        ips: 861_411,
        memory_avg_bytes: 232,
        reductions_avg: 724,
        wall_avg_us: 1.16,
        wall_median_us: 1.17,
        wall_p99_us: 1.5
      },
      "rt drain 100 project= miss | otel" => %{
        ips: 857_397,
        memory_avg_bytes: 232,
        reductions_avg: 724,
        wall_avg_us: 1.17,
        wall_median_us: 1.17,
        wall_p99_us: 1.5
      },
      "rt drain 1000 project= match | edge" => %{
        ips: 89_272,
        memory_avg_bytes: 384,
        reductions_avg: 7034,
        wall_avg_us: 11.2,
        wall_median_us: 10.67,
        wall_p99_us: 26.33
      },
      "rt drain 1000 project= match | otel" => %{
        ips: 89_904,
        memory_avg_bytes: 472,
        reductions_avg: 7055,
        wall_avg_us: 11.12,
        wall_median_us: 11.08,
        wall_p99_us: 14.67
      },
      "rt empty | edge" => %{
        ips: 18_695_940,
        memory_avg_bytes: 112,
        reductions_avg: 15,
        wall_avg_us: 0.05,
        wall_median_us: 0.04,
        wall_p99_us: 0.04
      },
      "rt empty | otel" => %{
        ips: 15_700_110,
        memory_avg_bytes: 112,
        reductions_avg: 15,
        wall_avg_us: 0.06,
        wall_median_us: 0.04,
        wall_p99_us: 0.04
      },
      "rt mixed 100 | edge" => %{
        ips: 188_181,
        memory_avg_bytes: 4808,
        reductions_avg: 1111,
        wall_avg_us: 5.31,
        wall_median_us: 5.13,
        wall_p99_us: 7.25
      },
      "rt mixed 100 | otel" => %{
        ips: 185_423,
        memory_avg_bytes: 4688,
        reductions_avg: 962,
        wall_avg_us: 5.39,
        wall_median_us: 5.38,
        wall_p99_us: 7.21
      },
      "rt regex only 100 | edge" => %{
        ips: 33_022,
        memory_avg_bytes: 3448,
        reductions_avg: 3244,
        wall_avg_us: 30.28,
        wall_median_us: 30.17,
        wall_p99_us: 39.88
      },
      "rt regex only 100 | otel" => %{
        ips: 33_237,
        memory_avg_bytes: 3448,
        reductions_avg: 2024,
        wall_avg_us: 30.09,
        wall_median_us: 30.33,
        wall_p99_us: 36.0
      },
      "rt scattered 100 path= match | edge" => %{
        ips: 243_689,
        memory_avg_bytes: 6512,
        reductions_avg: 668,
        wall_avg_us: 4.1,
        wall_median_us: 4.0,
        wall_p99_us: 5.83
      },
      "rt scattered 100 path= match | otel" => %{
        ips: 173_167,
        memory_avg_bytes: 6512,
        reductions_avg: 415,
        wall_avg_us: 5.77,
        wall_median_us: 5.67,
        wall_p99_us: 7.79
      }
    }
  }
]
