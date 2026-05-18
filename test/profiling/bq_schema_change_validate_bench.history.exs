[
  %{
    meta: %{
      captured_at: "2026-05-18T16:07:16.468644Z",
      git_sha: "8d6838a9a",
      label: "o11y-1829 drop is_binary join_key",
      machine: "Apple M5"
    },
    results: %{
      "empty schema short-circuit" => %{
        ips: 1_257_155,
        memory_avg_bytes: 968,
        reductions_avg: 134,
        wall_avg_us: 0.8,
        wall_median_us: 0.5,
        wall_p99_us: 5.54
      },
      "lists (third_with_lists)" => %{
        ips: 171_710,
        memory_avg_bytes: 3272,
        reductions_avg: 552,
        wall_avg_us: 5.82,
        wall_median_us: 4.63,
        wall_p99_us: 25.46
      },
      "scalars (third)" => %{
        ips: 190_613,
        memory_avg_bytes: 2592,
        reductions_avg: 526,
        wall_avg_us: 5.25,
        wall_median_us: 4.21,
        wall_p99_us: 18.29
      }
    }
  },
  %{
    meta: %{
      captured_at: "2026-05-18T15:50:24.421713Z",
      git_sha: "236d147ff",
      label: "o11y-1829 tightened validate loop",
      machine: "Apple M5"
    },
    results: %{
      "empty schema short-circuit" => %{
        ips: 1_214_845,
        memory_avg_bytes: 968,
        reductions_avg: 134,
        wall_avg_us: 0.82,
        wall_median_us: 0.54,
        wall_p99_us: 5.17
      },
      "lists (third_with_lists)" => %{
        ips: 160_477,
        memory_avg_bytes: 4400,
        reductions_avg: 552,
        wall_avg_us: 6.23,
        wall_median_us: 5.0,
        wall_p99_us: 27.21
      },
      "scalars (third)" => %{
        ips: 176_945,
        memory_avg_bytes: 3648,
        reductions_avg: 526,
        wall_avg_us: 5.65,
        wall_median_us: 4.63,
        wall_p99_us: 26.08
      }
    }
  },
  %{
    meta: %{
      captured_at: "2026-05-18T15:49:19.206556Z",
      git_sha: "640151ae9",
      label: "o11y-1829 single-pass validate",
      machine: "Apple M5"
    },
    results: %{
      "empty schema short-circuit" => %{
        ips: 1_231_415,
        memory_avg_bytes: 968,
        reductions_avg: 133,
        wall_avg_us: 0.81,
        wall_median_us: 0.5,
        wall_p99_us: 5.42
      },
      "lists (third_with_lists)" => %{
        ips: 164_896,
        memory_avg_bytes: 4008,
        reductions_avg: 669,
        wall_avg_us: 6.06,
        wall_median_us: 4.83,
        wall_p99_us: 28.71
      },
      "scalars (third)" => %{
        ips: 200_337,
        memory_avg_bytes: 3296,
        reductions_avg: 636,
        wall_avg_us: 4.99,
        wall_median_us: 4.29,
        wall_p99_us: 14.75
      }
    }
  },
  %{
    meta: %{
      captured_at: "2026-05-18T15:48:07.409482Z",
      git_sha: "8e03624a4",
      label: "o11y-1829 baseline (main)",
      machine: "Apple M5"
    },
    results: %{
      "empty schema short-circuit" => %{
        ips: 230_488,
        memory_avg_bytes: 10_752,
        reductions_avg: 951,
        wall_avg_us: 4.34,
        wall_median_us: 3.67,
        wall_p99_us: 9.42
      },
      "lists (third_with_lists)" => %{
        ips: 111_634,
        memory_avg_bytes: 15_752,
        reductions_avg: 1349,
        wall_avg_us: 8.96,
        wall_median_us: 8.13,
        wall_p99_us: 21.17
      },
      "scalars (third)" => %{
        ips: 132_128,
        memory_avg_bytes: 14_088,
        reductions_avg: 1255,
        wall_avg_us: 7.57,
        wall_median_us: 6.63,
        wall_p99_us: 22.21
      }
    }
  }
]
