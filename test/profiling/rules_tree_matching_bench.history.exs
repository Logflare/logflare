[
  %{
    meta: %{
      captured_at: "2026-06-01T14:41:59.833094Z",
      git_sha: "655e83a02",
      label: "eq_index leaf + 1f fast-path",
      machine: nil
    },
    results: %{
      "rt drain 100 project= match | edge" => %{
        ips: 6_298_538,
        memory_avg_bytes: 384,
        reductions_avg: 41,
        wall_avg_us: 0.16,
        wall_median_us: 0.13,
        wall_p99_us: 0.25
      },
      "rt drain 100 project= match | otel" => %{
        ips: 2_222_597,
        memory_avg_bytes: 472,
        reductions_avg: 62,
        wall_avg_us: 0.45,
        wall_median_us: 0.42,
        wall_p99_us: 0.58
      },
      "rt drain 100 project= miss | edge" => %{
        ips: 7_580_880,
        memory_avg_bytes: 232,
        reductions_avg: 31,
        wall_avg_us: 0.13,
        wall_median_us: 0.13,
        wall_p99_us: 0.21
      },
      "rt drain 100 project= miss | otel" => %{
        ips: 5_878_007,
        memory_avg_bytes: 232,
        reductions_avg: 31,
        wall_avg_us: 0.17,
        wall_median_us: 0.13,
        wall_p99_us: 0.25
      },
      "rt drain 1000 project= match | edge" => %{
        ips: 6_010_043,
        memory_avg_bytes: 384,
        reductions_avg: 41,
        wall_avg_us: 0.17,
        wall_median_us: 0.13,
        wall_p99_us: 0.25
      },
      "rt drain 1000 project= match | otel" => %{
        ips: 2_235_153,
        memory_avg_bytes: 472,
        reductions_avg: 62,
        wall_avg_us: 0.45,
        wall_median_us: 0.42,
        wall_p99_us: 0.58
      },
      "rt empty | edge" => %{
        ips: 46_115_422,
        memory_avg_bytes: 112,
        reductions_avg: 15,
        wall_avg_us: 0.02,
        wall_median_us: 0.02,
        wall_p99_us: 0.03
      },
      "rt empty | otel" => %{
        ips: 17_836_628,
        memory_avg_bytes: 112,
        reductions_avg: 15,
        wall_avg_us: 0.06,
        wall_median_us: 0.04,
        wall_p99_us: 0.04
      },
      "rt mixed 100 | edge" => %{
        ips: 219_135,
        memory_avg_bytes: 4808,
        reductions_avg: 684,
        wall_avg_us: 4.56,
        wall_median_us: 4.42,
        wall_p99_us: 6.29
      },
      "rt mixed 100 | otel" => %{
        ips: 218_915,
        memory_avg_bytes: 4688,
        reductions_avg: 535,
        wall_avg_us: 4.57,
        wall_median_us: 4.46,
        wall_p99_us: 6.29
      },
      "rt regex only 100 | edge" => %{
        ips: 33_452,
        memory_avg_bytes: 3448,
        reductions_avg: 3244,
        wall_avg_us: 29.89,
        wall_median_us: 30.08,
        wall_p99_us: 38.13
      },
      "rt regex only 100 | otel" => %{
        ips: 33_185,
        memory_avg_bytes: 3448,
        reductions_avg: 2024,
        wall_avg_us: 30.13,
        wall_median_us: 30.21,
        wall_p99_us: 37.79
      },
      "rt scattered 100 path= match | edge" => %{
        ips: 239_848,
        memory_avg_bytes: 6512,
        reductions_avg: 571,
        wall_avg_us: 4.17,
        wall_median_us: 4.0,
        wall_p99_us: 17.04
      },
      "rt scattered 100 path= match | otel" => %{
        ips: 177_682,
        memory_avg_bytes: 6512,
        reductions_avg: 415,
        wall_avg_us: 5.63,
        wall_median_us: 5.58,
        wall_p99_us: 7.63
      }
    }
  },
  %{
    meta: %{
      captured_at: "2026-06-01T11:18:28.826075Z",
      git_sha: "458d32d97",
      label: "eq_index leaf",
      machine: nil
    },
    results: %{
      "rt drain 100 project= match | edge" => %{
        ips: 5_744_282,
        memory_avg_bytes: 384,
        reductions_avg: 41,
        wall_avg_us: 0.17,
        wall_median_us: 0.17,
        wall_p99_us: 0.25
      },
      "rt drain 100 project= match | otel" => %{
        ips: 1_995_257,
        memory_avg_bytes: 472,
        reductions_avg: 62,
        wall_avg_us: 0.5,
        wall_median_us: 0.42,
        wall_p99_us: 0.58
      },
      "rt drain 100 project= miss | edge" => %{
        ips: 7_042_912,
        memory_avg_bytes: 232,
        reductions_avg: 31,
        wall_avg_us: 0.14,
        wall_median_us: 0.13,
        wall_p99_us: 0.25
      },
      "rt drain 100 project= miss | otel" => %{
        ips: 6_052_452,
        memory_avg_bytes: 232,
        reductions_avg: 31,
        wall_avg_us: 0.17,
        wall_median_us: 0.17,
        wall_p99_us: 0.25
      },
      "rt drain 1000 project= match | edge" => %{
        ips: 5_718_106,
        memory_avg_bytes: 384,
        reductions_avg: 41,
        wall_avg_us: 0.17,
        wall_median_us: 0.17,
        wall_p99_us: 0.29
      },
      "rt drain 1000 project= match | otel" => %{
        ips: 2_200_268,
        memory_avg_bytes: 472,
        reductions_avg: 62,
        wall_avg_us: 0.45,
        wall_median_us: 0.42,
        wall_p99_us: 0.58
      },
      "rt empty | edge" => %{
        ips: 44_355_858,
        memory_avg_bytes: 112,
        reductions_avg: 15,
        wall_avg_us: 0.02,
        wall_median_us: 0.02,
        wall_p99_us: 0.03
      },
      "rt empty | otel" => %{
        ips: 45_877_080,
        memory_avg_bytes: 112,
        reductions_avg: 15,
        wall_avg_us: 0.02,
        wall_median_us: 0.02,
        wall_p99_us: 0.03
      },
      "rt mixed 100 | edge" => %{
        ips: 215_552,
        memory_avg_bytes: 4808,
        reductions_avg: 684,
        wall_avg_us: 4.64,
        wall_median_us: 4.58,
        wall_p99_us: 6.04
      },
      "rt mixed 100 | otel" => %{
        ips: 220_553,
        memory_avg_bytes: 4688,
        reductions_avg: 541,
        wall_avg_us: 4.53,
        wall_median_us: 4.5,
        wall_p99_us: 5.92
      },
      "rt regex only 100 | edge" => %{
        ips: 33_470,
        memory_avg_bytes: 3448,
        reductions_avg: 3244,
        wall_avg_us: 29.88,
        wall_median_us: 29.63,
        wall_p99_us: 38.5
      },
      "rt regex only 100 | otel" => %{
        ips: 34_131,
        memory_avg_bytes: 3448,
        reductions_avg: 2024,
        wall_avg_us: 29.3,
        wall_median_us: 28.88,
        wall_p99_us: 44.83
      },
      "rt scattered 100 path= match | edge" => %{
        ips: 238_099,
        memory_avg_bytes: 6512,
        reductions_avg: 668,
        wall_avg_us: 4.2,
        wall_median_us: 4.13,
        wall_p99_us: 5.38
      },
      "rt scattered 100 path= match | otel" => %{
        ips: 167_219,
        memory_avg_bytes: 6512,
        reductions_avg: 415,
        wall_avg_us: 5.98,
        wall_median_us: 5.88,
        wall_p99_us: 7.75
      }
    }
  },
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
