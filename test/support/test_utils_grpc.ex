defmodule Logflare.TestUtilsGrpc do
  alias Logflare.TestUtils

  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceRequest
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest

  alias Opentelemetry.Proto.Common.V1.AnyValue
  alias Opentelemetry.Proto.Common.V1.ArrayValue
  alias Opentelemetry.Proto.Common.V1.InstrumentationScope
  alias Opentelemetry.Proto.Common.V1.KeyValue
  alias Opentelemetry.Proto.Common.V1.KeyValueList

  alias Opentelemetry.Proto.Resource.V1.Resource

  alias Opentelemetry.Proto.Trace.V1.ScopeSpans
  alias Opentelemetry.Proto.Trace.V1.Span
  alias Opentelemetry.Proto.Trace.V1.Span.Event
  alias Opentelemetry.Proto.Trace.V1.ResourceSpans

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

  alias Opentelemetry.Proto.Logs.V1.ResourceLogs
  alias Opentelemetry.Proto.Logs.V1.ScopeLogs
  alias Opentelemetry.Proto.Logs.V1.LogRecord

  @doc """
  Generates a ExportTraceServiceRequest message which contains a Span and an Event in it
  """
  def random_export_service_request do
    %ExportTraceServiceRequest{
      resource_spans: random_resource_span()
    }
  end

  @doc """
  Generates a single ResourceSpan message which contains a Span and an Event in it
  """
  def random_resource_span do
    [
      %ResourceSpans{
        resource: %Resource{
          attributes: random_attributes()
        },
        scope_spans: [random_scope_span()]
      }
    ]
  end

  defp random_scope do
    %InstrumentationScope{
      name: TestUtils.random_string(),
      version: TestUtils.random_string(),
      attributes: random_attributes()
    }
  end

  defp random_scope_span do
    scope = random_scope()

    scope_spans = %ScopeSpans{
      scope: scope,
      spans: random_span()
    }

    scope_spans
  end

  defp random_span do
    [
      %Span{
        name: TestUtils.random_string(),
        span_id: :crypto.strong_rand_bytes(8),
        parent_span_id: :crypto.strong_rand_bytes(8),
        trace_id: :crypto.strong_rand_bytes(16),
        start_time_unix_nano: DateTime.utc_now() |> DateTime.to_unix(:nanosecond),
        end_time_unix_nano: DateTime.utc_now() |> DateTime.to_unix(:nanosecond),
        events: random_event(),
        attributes: random_attributes()
      }
    ]
  end

  defp random_event do
    [
      %Event{
        name: TestUtils.random_string(),
        time_unix_nano: DateTime.utc_now() |> DateTime.to_unix(:nanosecond)
      }
    ]
  end

  defp random_attributes do
    string = random_key_value(:string)
    boolean = random_key_value(:boolean)
    integer = random_key_value(:integer)
    double = random_key_value(:double)
    array_of_strings = random_key_value(:array_of_strings)
    array_of_booleans = random_key_value(:array_of_booleans)
    array_of_integers = random_key_value(:array_of_integers)
    array_of_doubles = random_key_value(:array_of_doubles)

    [
      string,
      boolean,
      integer,
      double,
      array_of_strings,
      array_of_booleans,
      array_of_integers,
      array_of_doubles
    ]
  end

  # this is not supported by traces/metrics, only logs
  # https://github.com/open-telemetry/opentelemetry-specification/pull/2888
  # https://github.com/open-telemetry/opentelemetry-specification/issues/376
  defp random_key_value(:array_of_kv) do
    %KeyValue{
      key: "random_array_#{TestUtils.random_string()}",
      value: %AnyValue{
        value:
          {:kvlist_value,
           %KeyValueList{
             values: [
               random_key_value(:string),
               random_key_value(:boolean),
               random_key_value(:integer),
               random_key_value(:double)
             ]
           }}
      }
    }
  end

  defp random_key_value(:array_of_strings) do
    %KeyValue{
      key: "random_array_#{TestUtils.random_string()}",
      value: %AnyValue{
        value:
          {:array_value,
           %ArrayValue{
             values: [
               random_key_value(:string).value,
               random_key_value(:string).value,
               random_key_value(:string).value
             ]
           }}
      }
    }
  end

  defp random_key_value(:array_of_booleans) do
    %KeyValue{
      key: "random_array_#{TestUtils.random_string()}",
      value: %AnyValue{
        value:
          {:array_value,
           %ArrayValue{
             values: [
               random_key_value(:boolean).value,
               random_key_value(:boolean).value,
               random_key_value(:boolean).value
             ]
           }}
      }
    }
  end

  defp random_key_value(:array_of_doubles) do
    %KeyValue{
      key: "random_array_#{TestUtils.random_string()}",
      value: %AnyValue{
        value:
          {:array_value,
           %ArrayValue{
             values: [
               random_key_value(:double).value,
               random_key_value(:double).value
             ]
           }}
      }
    }
  end

  defp random_key_value(:array_of_integers) do
    %KeyValue{
      key: "random_array_#{TestUtils.random_string()}",
      value: %AnyValue{
        value:
          {:array_value,
           %ArrayValue{
             values: [
               random_key_value(:integer).value,
               random_key_value(:integer).value
             ]
           }}
      }
    }
  end

  defp random_key_value(:string) do
    %KeyValue{
      key: "random_string_#{TestUtils.random_string()}",
      value: %AnyValue{
        value: {:string_value, TestUtils.random_string()}
      }
    }
  end

  defp random_key_value(:boolean) do
    %KeyValue{
      key: "random_boolean_#{TestUtils.random_string()}",
      value: %AnyValue{value: {:bool_value, Enum.random([true, false])}}
    }
  end

  defp random_key_value(:integer) do
    %KeyValue{
      key: "random_integer_#{TestUtils.random_string()}",
      value: %AnyValue{value: {:int_value, :rand.uniform(100)}}
    }
  end

  defp random_key_value(:double) do
    %KeyValue{
      key: "random_double_#{TestUtils.random_string()}",
      value: %AnyValue{value: {:double_value, :rand.uniform(100) + 1.00}}
    }
  end

  @doc """
  Generates a (somewhat) random otel metrics request which contains all metrics types
  """
  def random_otel_metrics_request do
    %ExportMetricsServiceRequest{
      resource_metrics: [
        random_resource_metrics([:gauge, :histogram]),
        random_resource_metrics([:sum, :exponential_histogram])
      ]
    }
  end

  defp random_resource_metrics(metric_types) do
    %ResourceMetrics{
      resource: %Resource{attributes: random_attributes()},
      scope_metrics: [
        %ScopeMetrics{
          scope: random_scope(),
          metrics: Enum.map(metric_types, &random_metric/1)
        }
      ]
    }
  end

  defp random_metric(:gauge) do
    %Metric{
      name: "random_gauge_#{TestUtils.random_string()}",
      description: "random gauge description",
      unit: random_unit(),
      data: {:gauge, %Gauge{data_points: random_number_data_points()}}
    }
  end

  defp random_metric(:sum) do
    %Metric{
      name: "random_sum_#{TestUtils.random_string()}",
      description: "random sum description",
      unit: random_unit(),
      data:
        {:sum,
         %Sum{
           data_points: random_number_data_points(),
           aggregation_temporality:
             Enum.random([:AGGREGATION_TEMPORALITY_CUMULATIVE, :AGGREGATION_TEMPORALITY_DELTA]),
           is_monotonic: Enum.random([true, false])
         }}
    }
  end

  defp random_metric(:histogram) do
    %Metric{
      name: "random_histogram_#{TestUtils.random_string()}",
      description: "random histogram description",
      unit: random_unit(),
      data:
        {:histogram,
         %Histogram{
           data_points: random_histogram_data_points(),
           aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE
         }}
    }
  end

  defp random_metric(:exponential_histogram) do
    %Metric{
      name: "random_exponential_histogram_#{TestUtils.random_string()}",
      description: "random exponential histogram description",
      unit: random_unit(),
      data:
        {:exponential_histogram,
         %ExponentialHistogram{
           data_points: random_exponential_histogram_data_points(),
           aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE
         }}
    }
  end

  defp random_number_data_points do
    [
      %NumberDataPoint{
        attributes: random_attributes(),
        start_time_unix_nano: :os.system_time(:nanosecond),
        time_unix_nano: :os.system_time(:nanosecond),
        value: {:as_int, :rand.uniform(100)}
      }
    ]
  end

  defp random_histogram_data_points do
    [
      %HistogramDataPoint{
        attributes: random_attributes(),
        start_time_unix_nano: :os.system_time(:nanosecond),
        time_unix_nano: :os.system_time(:nanosecond),
        bucket_counts: Enum.map(0..10, fn _ -> :rand.uniform(100) end),
        count: :rand.uniform(100),
        sum: :rand.uniform(100)
      }
    ]
  end

  defp random_exponential_histogram_data_points do
    [
      %ExponentialHistogramDataPoint{
        attributes: random_attributes(),
        start_time_unix_nano: :os.system_time(:nanosecond),
        time_unix_nano: :os.system_time(:nanosecond),
        count: :rand.uniform(100),
        sum: :rand.uniform(100) * 1.0,
        scale: Enum.random(-10..10),
        zero_count: :rand.uniform(10),
        positive: %Opentelemetry.Proto.Metrics.V1.ExponentialHistogramDataPoint.Buckets{
          offset: 0,
          bucket_counts: Enum.map(1..5, fn _ -> :rand.uniform(100) end)
        },
        negative: %Opentelemetry.Proto.Metrics.V1.ExponentialHistogramDataPoint.Buckets{
          offset: 0,
          bucket_counts: Enum.map(1..5, fn _ -> :rand.uniform(100) end)
        },
        flags: 0,
        zero_threshold: 0.0
      }
    ]
  end

  defp random_unit, do: Enum.random(["s", "By", "ms"])

  @doc """
  Generates a ExportTraceServiceRequest message which contains a Span and an Event in it
  """
  def random_otel_logs_request do
    %ExportLogsServiceRequest{
      resource_logs: random_resource_logs()
    }
  end

  def random_resource_logs do
    [
      %ResourceLogs{
        resource: %Resource{
          attributes: random_attributes()
        },
        scope_logs: [random_scope_logs()]
      }
    ]
  end

  defp random_scope_logs do
    scope = random_scope()

    %ScopeLogs{
      scope: scope,
      log_records: [random_log_record()]
    }
  end

  defp random_log_record do
    %LogRecord{
      time_unix_nano: DateTime.utc_now() |> DateTime.to_unix(:nanosecond),
      severity_number: random_severity_number(),
      severity_text: random_severity_text(),
      body: %AnyValue{value: {:string_value, Logflare.TestUtils.random_string()}},
      attributes: random_attributes(),
      trace_id: :crypto.strong_rand_bytes(16),
      span_id: :crypto.strong_rand_bytes(8),
      event_name: TestUtils.random_string()
    }
  end

  defp random_severity_number do
    Enum.random([
      :SEVERITY_NUMBER_UNSPECIFIED,
      :SEVERITY_NUMBER_TRACE,
      :SEVERITY_NUMBER_INFO,
      :SEVERITY_NUMBER_WARN,
      :SEVERITY_NUMBER_ERROR,
      :SEVERITY_NUMBER_FATAL
    ])
  end

  defp random_severity_text do
    Enum.random([
      "TRACE",
      "DEBUG",
      "INFO",
      "WARN",
      "ERROR",
      "FATAL"
    ])
  end
end
