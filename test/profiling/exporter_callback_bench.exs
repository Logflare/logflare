# Benchmark script for Logflare.Backends.UserMonitoring exporter functions
#
# Run with: elixir test/profiling/exporter_callback_bench.exs
#
# This benchmarks:
# - extract_tags/2 - extracts string keys with non-nested values from metadata
# - exporter_callback/2 - called periodically to export OTEL metrics

alias Logflare.Backends.UserMonitoring
alias Logflare.Logs.Processor
alias Logflare.Sources
alias Logflare.Users

import Logflare.Factory

# Setup Mimic to stub Processor.ingest to avoid actual ingestion
Mimic.copy(Logflare.Logs.Processor)
Mimic.stub(Processor, :ingest, fn _, _, _ -> :ok end)

IO.puts("Setting up test data...")

# Create users with different monitoring states
user_with_monitoring = insert(:user, system_monitoring: true)

# Create sources for the users
source_monitored = insert(:source, user: user_with_monitoring)

# Create system sources for the monitored user
_logs_system_source =
  insert(:source,
    user: user_with_monitoring,
    system_source: true,
    system_source_type: :logs
  )

_metrics_system_source =
  insert(:source,
    user: user_with_monitoring,
    system_source: true,
    system_source_type: :metrics
  )

# Warm the caches
Users.Cache.get(user_with_monitoring.id)
Sources.Cache.get_by_id(source_monitored.id)
Sources.Cache.get_source_by_token(source_monitored.token)
Sources.Cache.get_by(user_id: user_with_monitoring.id, system_source_type: :logs)
Sources.Cache.get_by(user_id: user_with_monitoring.id, system_source_type: :metrics)
# Dummy metric (first arg is ignored)
dummy_metric = :ignored

# ============================================================================
# Build test inputs for exporter_callback - using proper OTEL Protobuf structs
# ============================================================================

alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.Metric
alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.Sum
alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.NumberDataPoint
alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.KeyValue
alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.AnyValue

defmodule OtelMetricBuilder do
  @moduledoc false

  @default_datapoints_per_metric 100

  def build_metrics(
        count,
        source_id,
        user_id,
        datapoints_per_metric \\ @default_datapoints_per_metric
      ) do
    for i <- 1..count do
      %Metric{
        name: "logflare.backends.ingest.ingested_bytes",
        description: "Amount of bytes ingested",
        unit: "bytes",
        data:
          {:sum,
           %Sum{
             data_points: build_datapoints(datapoints_per_metric, source_id, user_id, i),
             aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE,
             is_monotonic: true
           }}
      }
    end
  end

  defp build_datapoints(count, source_id, user_id, metric_index) do
    base_time = System.system_time(:nanosecond)

    for j <- 1..count do
      %NumberDataPoint{
        start_time_unix_nano: base_time + j * 1_000_000,
        time_unix_nano: base_time + j * 1_000_000 + 500_000,
        value: {:as_int, 1024 * metric_index * j},
        attributes: [
          %KeyValue{
            key: "source_id",
            value: %AnyValue{value: {:int_value, source_id}}
          },
          %KeyValue{
            key: "user_id",
            value: %AnyValue{value: {:int_value, user_id}}
          },
          %KeyValue{
            key: "datapoint_index",
            value: %AnyValue{value: {:int_value, j}}
          }
        ]
      }
    end
  end
end

exporter_config = %{
  resource: %{
    name: "Logflare",
    service: %{
      name: "Logflare",
      version: "1.0.0-bench"
    },
    node: inspect(Node.self()),
    cluster: "benchmark"
  }
}

metrics_1 = OtelMetricBuilder.build_metrics(1, source_monitored.id, user_with_monitoring.id)
metrics_50 = OtelMetricBuilder.build_metrics(50, source_monitored.id, user_with_monitoring.id)
metrics_500 = OtelMetricBuilder.build_metrics(500, source_monitored.id, user_with_monitoring.id)
metrics_1k = OtelMetricBuilder.build_metrics(1_000, source_monitored.id, user_with_monitoring.id)
metrics_5k = OtelMetricBuilder.build_metrics(5_000, source_monitored.id, user_with_monitoring.id)

metrics_10k =
  OtelMetricBuilder.build_metrics(10_000, source_monitored.id, user_with_monitoring.id)

# ============================================================================
# Benchmark: exporter_callback/2
# ============================================================================
IO.puts("\n" <> String.duplicate("=", 70))
IO.puts("Benchmarking exporter_callback/2")
IO.puts(String.duplicate("=", 70))

Benchee.run(
  %{
    "exporter_callback - no flow`" => fn {metrics, config} ->
      UserMonitoring.exporter_callback({:metrics, metrics}, config, flow: false)
    end,
    "exporter_callback - flow" => fn {metrics, config} ->
      UserMonitoring.exporter_callback({:metrics, metrics}, config, flow: true)
    end
  },
  inputs: %{
    "1 metric" => {metrics_1, exporter_config},
    "50 metrics" => {metrics_50, exporter_config},
    "500 metrics" => {metrics_500, exporter_config},
    "1k metrics" => {metrics_1k, exporter_config},
    "5k metrics" => {metrics_5k, exporter_config},
    "10k metrics" => {metrics_10k, exporter_config}
  },
  time: 3,
  warmup: 1
)

IO.puts("\nBenchmarks complete!")
