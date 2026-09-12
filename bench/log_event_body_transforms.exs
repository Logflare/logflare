# Run from the repository root with:
#   MIX_ENV=dev ../bin/x mix run --no-start bench/log_event_body_transforms.exs
#
# Compare this command unchanged on the benchmark-only baseline commit and its
# body-transform implementation child.

alias Logflare.LogEvent
alias Logflare.Sources.Source

iterations_seconds = String.to_integer(System.get_env("LF_BENCH_TIME") || "5")
warmup_seconds = String.to_integer(System.get_env("LF_BENCH_WARMUP") || "2")
memory_seconds = String.to_integer(System.get_env("LF_BENCH_MEMORY_TIME") || "2")

params = %{
  "id" => "00000000-0000-0000-0000-000000000001",
  "timestamp" => 1_725_000_000_123_456,
  "event_message" => "request completed",
  "metadata" => %{
    "request" => %{
      "method" => "POST",
      "path" => "/logs",
      "headers" => %{"content-type" => "application/json"}
    },
    "response" => %{"status" => 202, "duration_ms" => 14},
    "debug" => %{"attempt" => 1}
  },
  "service" => %{"name" => "api", "version" => "1.2.3"},
  "unused" => "remove me"
}

source = %Source{
  id: 1,
  token: "00000000-0000-0000-0000-000000000002",
  name: "log-event-transform-benchmark",
  validate_schema: false
}

transform_source = %{
  source
  | transform_copy_fields_parsed: [
      %{
        from_path: ["metadata", "request", "method"],
        to_path: ["request", "method"]
      },
      %{
        from_path: ["metadata", "response", "status"],
        to_path: ["response", "status"]
      },
      %{
        from_path: ["service", "version"],
        to_path: ["deployment", "version"]
      }
    ],
    transform_key_values_parsed: [],
    transform_drop_fields_parsed: [
      ["metadata", "debug"],
      ["metadata", "missing", "nested"],
      ["unused"]
    ]
}

Benchee.run(
  %{
    "no transforms" => fn -> LogEvent.make(params, %{source: source}) end,
    "three copies and three drops" => fn ->
      LogEvent.make(params, %{source: transform_source})
    end
  },
  time: iterations_seconds,
  warmup: warmup_seconds,
  memory_time: memory_seconds,
  parallel: 1,
  print: [benchmarking: true, configuration: true, fast_warning: false]
)
