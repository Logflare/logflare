# Benchmark script for Logflare.Logs.OtelMetric
#
# Run with: elixir test/profiling/otel_metric_bench.exs
#
# This benchmarks handle_batch/2 which converts OTEL ResourceMetrics to Logflare events

alias Logflare.Logs.OtelMetric

alias Opentelemetry.Proto.Common.V1.AnyValue
alias Opentelemetry.Proto.Common.V1.InstrumentationScope
alias Opentelemetry.Proto.Common.V1.KeyValue

alias Opentelemetry.Proto.Resource.V1.Resource

alias Opentelemetry.Proto.Metrics.V1.ResourceMetrics
alias Opentelemetry.Proto.Metrics.V1.ScopeMetrics
alias Opentelemetry.Proto.Metrics.V1.Metric
alias Opentelemetry.Proto.Metrics.V1.Gauge
alias Opentelemetry.Proto.Metrics.V1.Sum
alias Opentelemetry.Proto.Metrics.V1.Histogram
alias Opentelemetry.Proto.Metrics.V1.ExponentialHistogram
alias Opentelemetry.Proto.Metrics.V1.NumberDataPoint
alias Opentelemetry.Proto.Metrics.V1.HistogramDataPoint
alias Opentelemetry.Proto.Metrics.V1.ExponentialHistogramDataPoint

import Logflare.Factory

user = insert(:user)
source = insert(:source, user: user)

# ============================================================================
# Metric builders - scalable OTEL protobuf struct generation
# ============================================================================

defmodule OtelMetricBenchBuilder do
  @moduledoc false

  alias Opentelemetry.Proto.Common.V1.AnyValue
  alias Opentelemetry.Proto.Common.V1.InstrumentationScope
  alias Opentelemetry.Proto.Common.V1.KeyValue
  alias Opentelemetry.Proto.Metrics.V1.ExponentialHistogram
  alias Opentelemetry.Proto.Metrics.V1.ExponentialHistogramDataPoint
  alias Opentelemetry.Proto.Metrics.V1.Gauge
  alias Opentelemetry.Proto.Metrics.V1.Histogram
  alias Opentelemetry.Proto.Metrics.V1.HistogramDataPoint
  alias Opentelemetry.Proto.Metrics.V1.Metric
  alias Opentelemetry.Proto.Metrics.V1.NumberDataPoint
  alias Opentelemetry.Proto.Metrics.V1.ResourceMetrics
  alias Opentelemetry.Proto.Metrics.V1.ScopeMetrics
  alias Opentelemetry.Proto.Metrics.V1.Sum
  alias Opentelemetry.Proto.Resource.V1.Resource

  def build_resource_metrics(metric_count, datapoints_per_metric, metric_type \\ :gauge) do
    [
      %ResourceMetrics{
        resource: build_resource(),
        scope_metrics: [
          %ScopeMetrics{
            scope: build_scope(),
            metrics: build_metrics(metric_count, datapoints_per_metric, metric_type)
          }
        ]
      }
    ]
  end

  def build_mixed_resource_metrics(metrics_per_type, datapoints_per_metric) do
    types = [:gauge, :sum, :histogram, :exponential_histogram]

    [
      %ResourceMetrics{
        resource: build_resource(),
        scope_metrics: [
          %ScopeMetrics{
            scope: build_scope(),
            metrics:
              Enum.flat_map(types, fn type ->
                build_metrics(metrics_per_type, datapoints_per_metric, type)
              end)
          }
        ]
      }
    ]
  end

  defp build_resource do
    %Resource{
      attributes: [
        %KeyValue{key: "service.name", value: %AnyValue{value: {:string_value, "bench-service"}}},
        %KeyValue{
          key: "service.version",
          value: %AnyValue{value: {:string_value, "1.0.0-bench"}}
        },
        %KeyValue{key: "host.name", value: %AnyValue{value: {:string_value, "bench-host"}}},
        %KeyValue{key: "deployment.environment", value: %AnyValue{value: {:string_value, "prod"}}}
      ]
    }
  end

  defp build_scope do
    %InstrumentationScope{
      name: "bench-scope",
      version: "1.0.0",
      attributes: [
        %KeyValue{key: "scope.attr", value: %AnyValue{value: {:string_value, "scope-value"}}}
      ]
    }
  end

  defp build_metrics(count, datapoints_per_metric, type) do
    for i <- 1..count do
      build_metric(i, datapoints_per_metric, type)
    end
  end

  defp build_metric(index, datapoints_count, :gauge) do
    %Metric{
      name: "bench.gauge.metric_#{index}",
      unit: "ms",
      data: {:gauge, %Gauge{data_points: build_number_datapoints(datapoints_count, index)}}
    }
  end

  defp build_metric(index, datapoints_count, :sum) do
    %Metric{
      name: "bench.sum.metric_#{index}",
      unit: "bytes",
      data:
        {:sum,
         %Sum{
           data_points: build_number_datapoints(datapoints_count, index),
           aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE,
           is_monotonic: true
         }}
    }
  end

  defp build_metric(index, datapoints_count, :histogram) do
    %Metric{
      name: "bench.histogram.metric_#{index}",
      unit: "ms",
      data:
        {:histogram,
         %Histogram{
           data_points: build_histogram_datapoints(datapoints_count, index),
           aggregation_temporality: :AGGREGATION_TEMPORALITY_DELTA
         }}
    }
  end

  defp build_metric(index, datapoints_count, :exponential_histogram) do
    %Metric{
      name: "bench.exponential_histogram.metric_#{index}",
      unit: "ms",
      data:
        {:exponential_histogram,
         %ExponentialHistogram{
           data_points: build_exp_histogram_datapoints(datapoints_count, index),
           aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE
         }}
    }
  end

  defp build_number_datapoints(count, metric_index) do
    base_time = System.system_time(:nanosecond)

    for i <- 1..count do
      %NumberDataPoint{
        attributes: build_datapoint_attributes(metric_index, i),
        start_time_unix_nano: base_time + i * 1_000_000,
        time_unix_nano: base_time + i * 1_000_000 + 500_000,
        value: {:as_int, 100 * metric_index * i}
      }
    end
  end

  defp build_histogram_datapoints(count, metric_index) do
    base_time = System.system_time(:nanosecond)

    for i <- 1..count do
      %HistogramDataPoint{
        attributes: build_datapoint_attributes(metric_index, i),
        start_time_unix_nano: base_time + i * 1_000_000,
        time_unix_nano: base_time + i * 1_000_000 + 500_000,
        count: 100 * i,
        sum: 1000.0 * i,
        min: 1.0,
        max: 100.0 * i,
        bucket_counts: [10, 20, 30, 25, 10, 5],
        explicit_bounds: [0.0, 5.0, 10.0, 25.0, 50.0]
      }
    end
  end

  defp build_exp_histogram_datapoints(count, metric_index) do
    base_time = System.system_time(:nanosecond)

    for i <- 1..count do
      %ExponentialHistogramDataPoint{
        attributes: build_datapoint_attributes(metric_index, i),
        start_time_unix_nano: base_time + i * 1_000_000,
        time_unix_nano: base_time + i * 1_000_000 + 500_000,
        count: 100 * i,
        sum: 1000.0 * i,
        scale: 3,
        zero_count: 5,
        zero_threshold: 0.0,
        positive: %Opentelemetry.Proto.Metrics.V1.ExponentialHistogramDataPoint.Buckets{
          offset: 0,
          bucket_counts: [10, 20, 30, 25, 10]
        },
        negative: %Opentelemetry.Proto.Metrics.V1.ExponentialHistogramDataPoint.Buckets{
          offset: 0,
          bucket_counts: [5, 10, 15, 10, 5]
        },
        min: 1.0,
        max: 100.0 * i
      }
    end
  end

  defp build_datapoint_attributes(metric_index, datapoint_index) do
    [
      %KeyValue{key: "metric_idx", value: %AnyValue{value: {:int_value, metric_index}}},
      %KeyValue{key: "dp_idx", value: %AnyValue{value: {:int_value, datapoint_index}}},
      %KeyValue{key: "region", value: %AnyValue{value: {:string_value, "us-east-1"}}},
      %KeyValue{key: "instance", value: %AnyValue{value: {:string_value, "i-abc123"}}}
    ]
  end
end

# ============================================================================
# Build test inputs
# ============================================================================

# Mixed metric types
mixed_5m_1kdp = OtelMetricBenchBuilder.build_mixed_resource_metrics(5, 1000)

IO.puts("\n=== Benchmark: handle_batch/2 - Metric types comparison ===\n")

Benchee.run(
  %{
    "handle_batch" => fn resource_metrics ->
      OtelMetric.handle_batch(resource_metrics, source)
    end,
  },
  inputs: %{
    "mixed (5 each x 1kdp)" => mixed_5m_1kdp
  },
  time: 5,
  warmup: 2,
  memory_time: 2,
  reduction_time: 2
)
