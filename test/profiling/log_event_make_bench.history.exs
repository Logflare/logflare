[
  %{
    meta: %{
      captured_at: "2026-05-20T23:19:30.559366Z",
      git_sha: "62aaa6ef0",
      label: "m4 baseline pre-o11y-1858",
      machine: "m4-32gb"
    },
    results: %{
      "edge log" => %{
        ips: 14_710,
        memory_avg_bytes: 82_456,
        reductions_avg: 5623,
        wall_avg_us: 67.98,
        wall_median_us: 67.71,
        wall_p99_us: 78.92
      },
      "edge log + all" => %{
        ips: 13_975,
        memory_avg_bytes: 81_000,
        reductions_avg: 5523,
        wall_avg_us: 71.56,
        wall_median_us: 71.33,
        wall_p99_us: 82.21
      },
      "edge log + copy" => %{
        ips: 14_235,
        memory_avg_bytes: 84_112,
        reductions_avg: 5758,
        wall_avg_us: 70.25,
        wall_median_us: 69.88,
        wall_p99_us: 81.0
      },
      "edge log + copy + kv" => %{
        ips: 13_758,
        memory_avg_bytes: 86_104,
        reductions_avg: 5850,
        wall_avg_us: 72.69,
        wall_median_us: 72.33,
        wall_p99_us: 82.5
      },
      "edge log + drop" => %{
        ips: 14_582,
        memory_avg_bytes: 74_896,
        reductions_avg: 5245,
        wall_avg_us: 68.58,
        wall_median_us: 68.17,
        wall_p99_us: 78.17
      },
      "edge log + kv" => %{
        ips: 13_998,
        memory_avg_bytes: 84_048,
        reductions_avg: 5668,
        wall_avg_us: 71.44,
        wall_median_us: 70.92,
        wall_p99_us: 82.19
      },
      "otel trace" => %{
        ips: 32_860,
        memory_avg_bytes: 31_926,
        reductions_avg: 2508,
        wall_avg_us: 30.43,
        wall_median_us: 31.04,
        wall_p99_us: 36.21
      },
      "otel trace + all" => %{
        ips: 30_394,
        memory_avg_bytes: 35_726,
        reductions_avg: 2664,
        wall_avg_us: 32.9,
        wall_median_us: 32.63,
        wall_p99_us: 37.75
      },
      "otel trace + copy" => %{
        ips: 32_051,
        memory_avg_bytes: 33_598,
        reductions_avg: 2542,
        wall_avg_us: 31.2,
        wall_median_us: 31.42,
        wall_p99_us: 36.04
      },
      "otel trace + copy + kv" => %{
        ips: 30_400,
        memory_avg_bytes: 35_270,
        reductions_avg: 2751,
        wall_avg_us: 32.89,
        wall_median_us: 32.71,
        wall_p99_us: 37.38
      },
      "otel trace + drop" => %{
        ips: 31_906,
        memory_avg_bytes: 32_030,
        reductions_avg: 2486,
        wall_avg_us: 31.34,
        wall_median_us: 31.08,
        wall_p99_us: 35.71
      },
      "otel trace + kv" => %{
        ips: 30_615,
        memory_avg_bytes: 33_112,
        reductions_avg: 2586,
        wall_avg_us: 32.66,
        wall_median_us: 32.63,
        wall_p99_us: 37.79
      }
    }
  },
  %{
    meta: %{
      captured_at: "2026-05-28T14:59:24.259671Z",
      git_sha: "97f9f1104",
      label: "o11y-1910 post flattened_body removal",
      machine: "Apple M5"
    },
    results: %{
      "edge log" => %{
        ips: 19_318,
        memory_avg_bytes: 50_304,
        reductions_avg: 4062,
        wall_avg_us: 51.76,
        wall_median_us: 51.38,
        wall_p99_us: 60.79
      },
      "edge log + all" => %{
        ips: 18_458,
        memory_avg_bytes: 55_232,
        reductions_avg: 4418,
        wall_avg_us: 54.18,
        wall_median_us: 53.67,
        wall_p99_us: 63.21
      },
      "edge log + copy" => %{
        ips: 18_334,
        memory_avg_bytes: 49_464,
        reductions_avg: 4141,
        wall_avg_us: 54.54,
        wall_median_us: 53.17,
        wall_p99_us: 83.35
      },
      "edge log + copy + kv" => %{
        ips: 18_367,
        memory_avg_bytes: 51_888,
        reductions_avg: 4247,
        wall_avg_us: 54.45,
        wall_median_us: 53.96,
        wall_p99_us: 64.17
      },
      "edge log + drop" => %{
        ips: 18_742,
        memory_avg_bytes: 50_304,
        reductions_avg: 4130,
        wall_avg_us: 53.36,
        wall_median_us: 52.96,
        wall_p99_us: 62.29
      },
      "edge log + kv" => %{
        ips: 18_530,
        memory_avg_bytes: 50_320,
        reductions_avg: 4204,
        wall_avg_us: 53.97,
        wall_median_us: 53.29,
        wall_p99_us: 65.58
      },
      "otel trace" => %{
        ips: 42_943,
        memory_avg_bytes: 24_662,
        reductions_avg: 2039,
        wall_avg_us: 23.29,
        wall_median_us: 22.75,
        wall_p99_us: 30.63
      },
      "otel trace + all" => %{
        ips: 39_213,
        memory_avg_bytes: 28_446,
        reductions_avg: 2285,
        wall_avg_us: 25.5,
        wall_median_us: 25.04,
        wall_p99_us: 32.83
      },
      "otel trace + copy" => %{
        ips: 41_790,
        memory_avg_bytes: 25_230,
        reductions_avg: 2091,
        wall_avg_us: 23.93,
        wall_median_us: 23.75,
        wall_p99_us: 30.08
      },
      "otel trace + copy + kv" => %{
        ips: 39_713,
        memory_avg_bytes: 26_456,
        reductions_avg: 2214,
        wall_avg_us: 25.18,
        wall_median_us: 24.83,
        wall_p99_us: 31.29
      },
      "otel trace + drop" => %{
        ips: 40_005,
        memory_avg_bytes: 26_070,
        reductions_avg: 2102,
        wall_avg_us: 25.0,
        wall_median_us: 24.92,
        wall_p99_us: 30.13
      },
      "otel trace + kv" => %{
        ips: 40_372,
        memory_avg_bytes: 26_544,
        reductions_avg: 2164,
        wall_avg_us: 24.77,
        wall_median_us: 24.54,
        wall_p99_us: 30.92
      }
    }
  },
  %{
    meta: %{
      captured_at: "2026-05-28T14:56:30.176977Z",
      git_sha: "7b2d6c250",
      label: "o11y-1910 baseline (pre flattened_body removal)",
      machine: "Apple M5"
    },
    results: %{
      "edge log" => %{
        ips: 17_398,
        memory_avg_bytes: 83_808,
        reductions_avg: 5621,
        wall_avg_us: 57.48,
        wall_median_us: 56.17,
        wall_p99_us: 81.25
      },
      "edge log + all" => %{
        ips: 17_214,
        memory_avg_bytes: 81_064,
        reductions_avg: 5522,
        wall_avg_us: 58.09,
        wall_median_us: 56.75,
        wall_p99_us: 77.99
      },
      "edge log + copy" => %{
        ips: 16_270,
        memory_avg_bytes: 84_112,
        reductions_avg: 5755,
        wall_avg_us: 61.46,
        wall_median_us: 57.25,
        wall_p99_us: 136.42
      },
      "edge log + copy + kv" => %{
        ips: 16_821,
        memory_avg_bytes: 86_544,
        reductions_avg: 5850,
        wall_avg_us: 59.45,
        wall_median_us: 58.58,
        wall_p99_us: 70.54
      },
      "edge log + drop" => %{
        ips: 17_970,
        memory_avg_bytes: 74_896,
        reductions_avg: 5244,
        wall_avg_us: 55.65,
        wall_median_us: 55.42,
        wall_p99_us: 65.33
      },
      "edge log + kv" => %{
        ips: 16_975,
        memory_avg_bytes: 84_048,
        reductions_avg: 5673,
        wall_avg_us: 58.91,
        wall_median_us: 58.46,
        wall_p99_us: 68.21
      },
      "otel trace" => %{
        ips: 41_821,
        memory_avg_bytes: 31_926,
        reductions_avg: 2496,
        wall_avg_us: 23.91,
        wall_median_us: 23.71,
        wall_p99_us: 29.5
      },
      "otel trace + all" => %{
        ips: 37_963,
        memory_avg_bytes: 35_726,
        reductions_avg: 2664,
        wall_avg_us: 26.34,
        wall_median_us: 25.88,
        wall_p99_us: 31.92
      },
      "otel trace + copy" => %{
        ips: 40_075,
        memory_avg_bytes: 33_505,
        reductions_avg: 2580,
        wall_avg_us: 24.95,
        wall_median_us: 24.75,
        wall_p99_us: 30.08
      },
      "otel trace + copy + kv" => %{
        ips: 38_084,
        memory_avg_bytes: 34_598,
        reductions_avg: 2750,
        wall_avg_us: 26.26,
        wall_median_us: 25.54,
        wall_p99_us: 36.5
      },
      "otel trace + drop" => %{
        ips: 39_134,
        memory_avg_bytes: 32_030,
        reductions_avg: 2431,
        wall_avg_us: 25.55,
        wall_median_us: 25.71,
        wall_p99_us: 31.04
      },
      "otel trace + kv" => %{
        ips: 39_034,
        memory_avg_bytes: 33_902,
        reductions_avg: 2586,
        wall_avg_us: 25.62,
        wall_median_us: 25.21,
        wall_p99_us: 31.75
      }
    }
  },
  %{
    meta: %{
      captured_at: "2026-05-18T15:28:15.216342Z",
      git_sha: "e76cd3b39",
      label: "o11y-1829 tightened validate loop",
      machine: "Apple M5"
    },
    results: %{
      "edge log" => %{
        ips: 17_954,
        memory_avg_bytes: 82_664,
        reductions_avg: 5584,
        wall_avg_us: 55.7,
        wall_median_us: 54.71,
        wall_p99_us: 68.0
      },
      "edge log + all" => %{
        ips: 16_972,
        memory_avg_bytes: 80_656,
        reductions_avg: 5675,
        wall_avg_us: 58.92,
        wall_median_us: 59.0,
        wall_p99_us: 70.0
      },
      "edge log + copy" => %{
        ips: 16_749,
        memory_avg_bytes: 84_112,
        reductions_avg: 5754,
        wall_avg_us: 59.7,
        wall_median_us: 59.71,
        wall_p99_us: 70.88
      },
      "edge log + copy + kv" => %{
        ips: 16_439,
        memory_avg_bytes: 86_544,
        reductions_avg: 5832,
        wall_avg_us: 60.83,
        wall_median_us: 60.79,
        wall_p99_us: 73.04
      },
      "edge log + drop" => %{
        ips: 16_716,
        memory_avg_bytes: 74_896,
        reductions_avg: 5244,
        wall_avg_us: 59.82,
        wall_median_us: 59.75,
        wall_p99_us: 69.5
      },
      "edge log + kv" => %{
        ips: 16_223,
        memory_avg_bytes: 84_016,
        reductions_avg: 5645,
        wall_avg_us: 61.64,
        wall_median_us: 61.42,
        wall_p99_us: 71.46
      },
      "otel trace" => %{
        ips: 40_278,
        memory_avg_bytes: 31_835,
        reductions_avg: 2512,
        wall_avg_us: 24.83,
        wall_median_us: 24.42,
        wall_p99_us: 31.29
      },
      "otel trace + all" => %{
        ips: 36_569,
        memory_avg_bytes: 35_726,
        reductions_avg: 2664,
        wall_avg_us: 27.35,
        wall_median_us: 27.29,
        wall_p99_us: 34.71
      },
      "otel trace + copy" => %{
        ips: 39_910,
        memory_avg_bytes: 32_867,
        reductions_avg: 2633,
        wall_avg_us: 25.06,
        wall_median_us: 24.79,
        wall_p99_us: 30.63
      },
      "otel trace + copy + kv" => %{
        ips: 37_871,
        memory_avg_bytes: 35_470,
        reductions_avg: 2755,
        wall_avg_us: 26.41,
        wall_median_us: 26.08,
        wall_p99_us: 32.88
      },
      "otel trace + drop" => %{
        ips: 39_736,
        memory_avg_bytes: 32_060,
        reductions_avg: 2470,
        wall_avg_us: 25.17,
        wall_median_us: 25.04,
        wall_p99_us: 30.96
      },
      "otel trace + kv" => %{
        ips: 39_272,
        memory_avg_bytes: 34_198,
        reductions_avg: 2622,
        wall_avg_us: 25.46,
        wall_median_us: 25.21,
        wall_p99_us: 31.58
      }
    }
  },
  %{
    meta: %{
      captured_at: "2026-05-16T20:44:01.594108Z",
      git_sha: "640151ae9",
      label: "o11y-1829 single-pass validate",
      machine: "Apple M5"
    },
    results: %{
      "edge log" => %{
        ips: 17_141,
        memory_avg_bytes: 83_808,
        reductions_avg: 5620,
        wall_avg_us: 58.34,
        wall_median_us: 57.75,
        wall_p99_us: 67.63
      },
      "edge log + all" => %{
        ips: 16_109,
        memory_avg_bytes: 81_064,
        reductions_avg: 5677,
        wall_avg_us: 62.08,
        wall_median_us: 61.79,
        wall_p99_us: 71.25
      },
      "edge log + copy" => %{
        ips: 16_528,
        memory_avg_bytes: 84_112,
        reductions_avg: 5754,
        wall_avg_us: 60.5,
        wall_median_us: 60.33,
        wall_p99_us: 70.83
      },
      "edge log + copy + kv" => %{
        ips: 15_846,
        memory_avg_bytes: 86_544,
        reductions_avg: 5913,
        wall_avg_us: 63.11,
        wall_median_us: 62.75,
        wall_p99_us: 72.97
      },
      "edge log + drop" => %{
        ips: 16_859,
        memory_avg_bytes: 74_896,
        reductions_avg: 5245,
        wall_avg_us: 59.32,
        wall_median_us: 59.08,
        wall_p99_us: 68.63
      },
      "edge log + kv" => %{
        ips: 16_209,
        memory_avg_bytes: 84_048,
        reductions_avg: 5679,
        wall_avg_us: 61.69,
        wall_median_us: 61.5,
        wall_p99_us: 70.54
      },
      "otel trace" => %{
        ips: 40_608,
        memory_avg_bytes: 31_926,
        reductions_avg: 2432,
        wall_avg_us: 24.63,
        wall_median_us: 24.13,
        wall_p99_us: 30.92
      },
      "otel trace + all" => %{
        ips: 37_229,
        memory_avg_bytes: 35_726,
        reductions_avg: 2664,
        wall_avg_us: 26.86,
        wall_median_us: 26.17,
        wall_p99_us: 32.88
      },
      "otel trace + copy" => %{
        ips: 38_782,
        memory_avg_bytes: 32_862,
        reductions_avg: 2584,
        wall_avg_us: 25.79,
        wall_median_us: 25.29,
        wall_p99_us: 31.71
      },
      "otel trace + copy + kv" => %{
        ips: 38_117,
        memory_avg_bytes: 34_709,
        reductions_avg: 2749,
        wall_avg_us: 26.24,
        wall_median_us: 25.88,
        wall_p99_us: 32.08
      },
      "otel trace + drop" => %{
        ips: 40_186,
        memory_avg_bytes: 32_030,
        reductions_avg: 2435,
        wall_avg_us: 24.88,
        wall_median_us: 24.67,
        wall_p99_us: 29.92
      },
      "otel trace + kv" => %{
        ips: 38_859,
        memory_avg_bytes: 33_938,
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
      machine: "Apple M5"
    },
    results: %{
      "edge log" => %{
        ips: 12_851,
        memory_avg_bytes: 141_968,
        reductions_avg: 9735,
        wall_avg_us: 77.82,
        wall_median_us: 77.25,
        wall_p99_us: 94.79
      },
      "edge log + all" => %{
        ips: 12_585,
        memory_avg_bytes: 130_088,
        reductions_avg: 9100,
        wall_avg_us: 79.46,
        wall_median_us: 79.54,
        wall_p99_us: 93.92
      },
      "edge log + copy" => %{
        ips: 12_499,
        memory_avg_bytes: 143_992,
        reductions_avg: 10_147,
        wall_avg_us: 80.01,
        wall_median_us: 79.38,
        wall_p99_us: 98.67
      },
      "edge log + copy + kv" => %{
        ips: 11_656,
        memory_avg_bytes: 146_496,
        reductions_avg: 10_314,
        wall_avg_us: 85.79,
        wall_median_us: 85.46,
        wall_p99_us: 103.5
      },
      "edge log + drop" => %{
        ips: 13_271,
        memory_avg_bytes: 121_560,
        reductions_avg: 8580,
        wall_avg_us: 75.35,
        wall_median_us: 74.58,
        wall_p99_us: 88.88
      },
      "edge log + kv" => %{
        ips: 12_056,
        memory_avg_bytes: 141_192,
        reductions_avg: 9799,
        wall_avg_us: 82.95,
        wall_median_us: 82.21,
        wall_p99_us: 101.1
      },
      "otel trace" => %{
        ips: 26_663,
        memory_avg_bytes: 57_782,
        reductions_avg: 4307,
        wall_avg_us: 37.5,
        wall_median_us: 37.54,
        wall_p99_us: 47.79
      },
      "otel trace + all" => %{
        ips: 25_396,
        memory_avg_bytes: 59_790,
        reductions_avg: 4436,
        wall_avg_us: 39.38,
        wall_median_us: 38.83,
        wall_p99_us: 46.96
      },
      "otel trace + copy" => %{
        ips: 25_335,
        memory_avg_bytes: 60_426,
        reductions_avg: 4632,
        wall_avg_us: 39.47,
        wall_median_us: 39.42,
        wall_p99_us: 47.04
      },
      "otel trace + copy + kv" => %{
        ips: 24_708,
        memory_avg_bytes: 63_742,
        reductions_avg: 4788,
        wall_avg_us: 40.47,
        wall_median_us: 40.54,
        wall_p99_us: 48.71
      },
      "otel trace + drop" => %{
        ips: 27_713,
        memory_avg_bytes: 51_050,
        reductions_avg: 3956,
        wall_avg_us: 36.08,
        wall_median_us: 35.71,
        wall_p99_us: 42.33
      },
      "otel trace + kv" => %{
        ips: 24_902,
        memory_avg_bytes: 60_350,
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
