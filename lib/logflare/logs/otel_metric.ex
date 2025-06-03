defmodule Logflare.Logs.OtelMetric do
  @moduledoc """
  Converts a Otel metric to event parameters

  Converts a list of ResourceMetrics to a list of Logflare Events

  Important details about the conversation:
  * One ResourceMetric can contain multiple ScopeSpans
  * One ScopeMetrics can contain multiple Metrics
  * One Metric can contain multiple DataPoints
  * A Log Event is created for each DataPoint, including parent metric data

  Supports gauges, sums, histograms and exponential histograms. Summaries aren't
  supported.
  """

  require Logger

  alias Opentelemetry.Proto.Metrics.V1.ResourceMetrics
  alias Opentelemetry.Proto.Metrics.V1.ScopeMetrics
  alias Opentelemetry.Proto.Metrics.V1.Metric

  @behaviour Logflare.Logs.Processor

  def handle_batch(resource_metrics, _source) when is_list(resource_metrics) do
    resource_metrics
    |> Enum.map(&handle_resource_metrics/1)
    |> List.flatten()
  end

  defp handle_resource_metrics(%ResourceMetrics{resource: resource, scope_metrics: scope_metrics}) do
    resource = Logflare.Logs.Otel.handle_resource(resource)

    scope_metrics
    |> Enum.map(&handle_scope_metric(&1, resource))
    |> List.flatten()
  end

  defp handle_scope_metric(%ScopeMetrics{scope: scope, metrics: metrics}, resource) do
    resource = Logflare.Logs.Otel.merge_scope_attributes(resource, scope)
    Enum.map(metrics, &handle_metric(&1, resource))
  end

  defp handle_metric(%Metric{} = metric, resource) do
    metadata = %{"type" => "metric"}
    metadata = Map.merge(metadata, resource)
    handle_metric_data(metric.data, metric, metadata)
  end

  defp handle_metric_data({:gauge, %{data_points: data_points}}, metric, metadata) do
    base = %{
      "metric_type" => "gauge",
      "event_message" => metric.name,
      "unit" => metric.unit,
      "metadata" => metadata,
      "project" => metadata["name"]
    }

    Enum.map(data_points, fn data_point ->
      %{value: {_, value}} = data_point

      Map.merge(base, %{
        "value" => value,
        "start_time" => Logflare.Logs.Otel.nano_to_iso8601(data_point.start_time_unix_nano),
        "attributes" => Logflare.Logs.Otel.handle_attributes(data_point.attributes),
        "timestamp" => Logflare.Logs.Otel.nano_to_iso8601(data_point.time_unix_nano)
      })
    end)
  end

  defp handle_metric_data({:sum, sum}, metric, metadata) do
    base = %{
      "metric_type" => "sum",
      "event_message" => metric.name,
      "unit" => metric.unit,
      "metadata" => metadata,
      "aggregation_temporality" => aggregation_temporality(sum.aggregation_temporality),
      "is_monotonic" => sum.is_monotonic,
      "project" => metadata["name"]
    }

    Enum.map(sum.data_points, fn data_point ->
      %{value: {_, value}} = data_point

      Map.merge(base, %{
        "value" => value,
        "start_time" => Logflare.Logs.Otel.nano_to_iso8601(data_point.start_time_unix_nano),
        "attributes" => Logflare.Logs.Otel.handle_attributes(data_point.attributes),
        "timestamp" => Logflare.Logs.Otel.nano_to_iso8601(data_point.time_unix_nano)
      })
    end)
  end

  defp handle_metric_data({:histogram, histogram}, metric, metadata) do
    base = %{
      "metric_type" => "histogram",
      "event_message" => metric.name,
      "unit" => metric.unit,
      "metadata" => metadata,
      "aggregation_temporality" => aggregation_temporality(histogram.aggregation_temporality),
      "project" => metadata["name"]
    }

    Enum.map(histogram.data_points, fn data_point ->
      Map.merge(base, %{
        "count" => data_point.count,
        "sum" => data_point.sum,
        "min" => data_point.min,
        "max" => data_point.max,
        "bucket_counts" => data_point.bucket_counts,
        "explicit_bounds" => data_point.explicit_bounds,
        "start_time" => Logflare.Logs.Otel.nano_to_iso8601(data_point.start_time_unix_nano),
        "attributes" => Logflare.Logs.Otel.handle_attributes(data_point.attributes),
        "timestamp" => Logflare.Logs.Otel.nano_to_iso8601(data_point.time_unix_nano)
      })
    end)
  end

  defp handle_metric_data({:exponential_histogram, histogram}, metric, metadata) do
    base = %{
      "metric_type" => "exponential_histogram",
      "event_message" => metric.name,
      "unit" => metric.unit,
      "metadata" => metadata,
      "aggregation_temporality" => aggregation_temporality(histogram.aggregation_temporality),
      "project" => metadata["name"]
    }

    Enum.map(histogram.data_points, fn data_point ->
      Map.merge(base, %{
        "count" => data_point.count,
        "sum" => data_point.sum,
        "scale" => data_point.scale,
        "zero_count" => data_point.scale,
        "zero_threshold" => data_point.zero_threshold,
        "positive" => exponential_histogram_buckets(data_point.positive),
        "negative" => exponential_histogram_buckets(data_point.negative),
        "min" => data_point.min,
        "max" => data_point.max,
        "start_time" => Logflare.Logs.Otel.nano_to_iso8601(data_point.start_time_unix_nano),
        "attributes" => Logflare.Logs.Otel.handle_attributes(data_point.attributes),
        "timestamp" => Logflare.Logs.Otel.nano_to_iso8601(data_point.time_unix_nano)
      })
    end)
  end

  defp handle_metric_data({type, _}, _, _) do
    Logger.warning("Unsupported metric type #{inspect(type)}, dropping")

    []
  end

  defp exponential_histogram_buckets(%{offset: offset, bucket_counts: bucket_counts}) do
    %{"offset" => offset, "bucket_counts" => bucket_counts}
  end

  defp aggregation_temporality(:AGGREGATION_TEMPORALITY_UNSPECIFIED), do: "unspecified"
  defp aggregation_temporality(:AGGREGATION_TEMPORALITY_DELTA), do: "delta"
  defp aggregation_temporality(:AGGREGATION_TEMPORALITY_CUMULATIVE), do: "cumulative"
end
