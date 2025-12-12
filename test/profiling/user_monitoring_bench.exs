# Benchmark script for Logflare.Backends.UserMonitoring functions
#
# Run with: elixir test/profiling/user_monitoring_bench.exs
#
# This benchmarks the key functions:
# - keep_metric_function/1 - called by Telemetry.Metrics for each metric
# - log_interceptor/2 - called by Logger for every log message
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

# Create users with different monitoring states
user_with_monitoring = insert(:user, system_monitoring: true)
user_without_monitoring = insert(:user, system_monitoring: false)

# Create sources for the users
source_monitored = insert(:source, user: user_with_monitoring)
source_not_monitored = insert(:source, user: user_without_monitoring)

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

# Create a backend and endpoint for testing all lookup paths
backend = insert(:backend, user: user_with_monitoring)
endpoint = insert(:endpoint, user: user_with_monitoring)

# Warm the caches
Users.Cache.get(user_with_monitoring.id)
Users.Cache.get(user_without_monitoring.id)
Sources.Cache.get_by_id(source_monitored.id)
Sources.Cache.get_by_id(source_not_monitored.id)
Sources.Cache.get_source_by_token(source_monitored.token)
Sources.Cache.get_by(user_id: user_with_monitoring.id, system_source_type: :logs)
Sources.Cache.get_by(user_id: user_with_monitoring.id, system_source_type: :metrics)
Logflare.Backends.Cache.get_backend(backend.id)
Logflare.Endpoints.Cache.get_endpoint_query(endpoint.id)

# ============================================================================
# Build test inputs
# ============================================================================

# keep_metric_function inputs
keep_metric_user_id_true = %{"user_id" => user_with_monitoring.id}
keep_metric_user_id_false = %{"user_id" => user_without_monitoring.id}
keep_metric_source_id_true = %{"source_id" => source_monitored.id}
keep_metric_source_id_false = %{"source_id" => source_not_monitored.id}
keep_metric_source_token = %{"source_token" => source_monitored.token}
keep_metric_backend_id = %{"backend_id" => backend.id}
keep_metric_endpoint_id = %{"endpoint_id" => endpoint.id}
keep_metric_no_user = %{"unrelated_key" => "value"}

# log_interceptor inputs
base_log_event = %{
  level: :info,
  msg: {:string, "test log message"},
  meta: %{
    pid: self(),
    gl: self(),
    time: System.system_time(:microsecond)
  }
}

log_event_full_path = put_in(base_log_event, [:meta, :source_id], source_monitored.id)
log_event_no_user = base_log_event
log_event_monitoring_false = put_in(base_log_event, [:meta, :source_id], source_not_monitored.id)
log_event_no_meta = Map.delete(base_log_event, :meta)

# exporter_callback inputs - using proper OTEL Protobuf structs
alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.Metric
alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.Sum
alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.NumberDataPoint
alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.KeyValue
alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.AnyValue

defmodule OtelMetricBuilder do
  @moduledoc false

  def build_metrics(count, source_id) do
    for i <- 1..count do
      %Metric{
        name: "logflare.backends.ingest.ingested_bytes",
        description: "Amount of bytes ingested",
        unit: "bytes",
        data:
          {:sum,
           %Sum{
             data_points: [
               %NumberDataPoint{
                 start_time_unix_nano: System.system_time(:nanosecond),
                 time_unix_nano: System.system_time(:nanosecond),
                 value: {:as_int, 1024 * i},
                 attributes: [
                   %KeyValue{
                     key: "source_id",
                     value: %AnyValue{value: {:int_value, source_id}}
                   }
                 ]
               }
             ],
             aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE,
             is_monotonic: true
           }}
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

metrics_1 = OtelMetricBuilder.build_metrics(1, source_monitored.id)
metrics_50 = OtelMetricBuilder.build_metrics(50, source_monitored.id)
metrics_500 = OtelMetricBuilder.build_metrics(500, source_monitored.id)
metrics_1k = OtelMetricBuilder.build_metrics(1_000, source_monitored.id)
metrics_5k = OtelMetricBuilder.build_metrics(5_000, source_monitored.id)
metrics_10k = OtelMetricBuilder.build_metrics(10_000, source_monitored.id)

# ============================================================================
# Benchmark: keep_metric_function/1
# ============================================================================

Benchee.run(
  %{
    "keep_metric_function" => fn input ->
      UserMonitoring.keep_metric_function(input)
    end
  },
  inputs: %{
    "user_id (monitoring=true)" => keep_metric_user_id_true,
    "user_id (monitoring=false)" => keep_metric_user_id_false,
    "source_id (monitoring=true)" => keep_metric_source_id_true,
    "source_id (monitoring=false)" => keep_metric_source_id_false,
    "source_token" => keep_metric_source_token,
    "backend_id" => keep_metric_backend_id,
    "endpoint_id" => keep_metric_endpoint_id,
    "no user (early return)" => keep_metric_no_user
  },
  time: 3,
  warmup: 1
)

# ============================================================================
# Benchmark: log_interceptor/2
# ============================================================================

Benchee.run(
  %{
    "log_interceptor" => fn input ->
      UserMonitoring.log_interceptor(input, [])
    end
  },
  inputs: %{
    "full path (ingests)" => log_event_full_path,
    "early return (no user_id)" => log_event_no_user,
    "early return (monitoring=false)" => log_event_monitoring_false,
    "early return (no meta)" => log_event_no_meta
  },
  time: 3,
  warmup: 1
)

# ============================================================================
# Benchmark: extract_tags/2
# ============================================================================

# extract_tags inputs - varying metadata sizes with string keys and mixed values
extract_tags_small = %{
  "source_id" => 123,
  "user_id" => 456,
  "backend_id" => 789
}

extract_tags_medium = %{
  "source_id" => 123,
  "user_id" => 456,
  "backend_id" => 789,
  "environment" => "production",
  "region" => "us-west-1",
  "service" => "api",
  "version" => "1.0.0",
  "host" => "server-01",
  "nested_map" => %{"should" => "be_filtered"},
  "nested_list" => [1, 2, 3]
}

extract_tags_large =
  Map.merge(
    extract_tags_medium,
    for i <- 1..50, into: %{} do
      {"tag_#{i}", "value_#{i}"}
    end
  )

extract_tags_with_nils = %{
  "source_id" => 123,
  "nil_value" => nil,
  "user_id" => 456,
  "another_nil" => nil,
  "backend_id" => 789
}

# Dummy metric (first arg is ignored)
dummy_metric = :ignored

Benchee.run(
  %{
    "extract_tags" => fn metadata ->
      UserMonitoring.extract_tags(dummy_metric, metadata)
    end
  },
  inputs: %{
    "small (3 keys)" => extract_tags_small,
    "medium (10 keys, mixed)" => extract_tags_medium,
    "large (60 keys)" => extract_tags_large,
    "with nils (5 keys)" => extract_tags_with_nils
  },
  time: 3,
  warmup: 1
)

# ============================================================================
# Benchmark: exporter_callback/2
# ============================================================================

Benchee.run(
  %{
    "exporter_callback" => fn {metrics, config} ->
      try do
        UserMonitoring.exporter_callback({:metrics, metrics}, config)
      rescue
        _ -> :skip
      end
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
