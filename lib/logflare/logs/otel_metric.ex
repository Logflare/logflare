defmodule Logflare.Logs.OtelMetric do
  @moduledoc """
  Converts a list of Otel ResourceMetrics to a list of Logflare events

  * One ResourceMetric can contain multiple ScopeSpans
  * One ScopeMetrics can contain multiple Metrics
  * One Metric can contain multiple DataPoints
  * A Log Event is created for each DataPoint, including parent metric data

  Supports gauges, sums, histograms and exponential histograms. Summaries aren't
  supported.
  """

  require Logger

  alias Logflare.Logs.Otel

  @behaviour Logflare.Logs.Processor

  def handle_batch(resource_metrics, _source) when is_list(resource_metrics) do
    resource_metrics
    |> Enum.map(&handle_resource_metrics/1)
    |> List.flatten()
  end

  def handle_resource_metrics(%_ResourceMetrics{resource: resource, scope_metrics: scope_metrics}) do
    resource = Otel.handle_resource(resource)
    Enum.map(scope_metrics, &handle_scope_metric(&1, resource))
  end

  def handle_scope_metric(%_ScopeMetrics{scope: scope, metrics: metrics}, resource) do
    scope = Otel.handle_scope(scope)
    Enum.map(metrics, &handle_metric(&1, resource, scope))
  end

  def handle_metric(%_Metric{name: name, unit: unit, data: data}, resource, scope) do
    base = %{
      "event_message" => name,
      "unit" => unit,
      "metadata" => %{"type" => "metric"},
      "scope" => scope,
      "resource" => resource,
      "project" => Otel.resource_project(resource)
    }

    handle_metric_data(data, base)
  end

  defp handle_metric_data({:gauge, %{data_points: data_points}}, base) do
    base = Map.merge(base, %{"metric_type" => "gauge"})

    Enum.map(data_points, fn data_point ->
      %{value: {_, value}} = data_point

      Map.merge(base, %{
        "value" => value,
        "start_time" => Otel.nano_to_iso8601(data_point.start_time_unix_nano),
        "attributes" => Otel.handle_attributes(data_point.attributes),
        "timestamp" => Otel.nano_to_iso8601(data_point.time_unix_nano)
      })
    end)
  end

  defp handle_metric_data({:sum, sum}, base) do
    base =
      Map.merge(base, %{
        "metric_type" => "sum",
        "aggregation_temporality" => aggregation_temporality(sum.aggregation_temporality),
        "is_monotonic" => sum.is_monotonic
      })

    Enum.map(sum.data_points, fn data_point ->
      %{value: {_, value}} = data_point

      Map.merge(base, %{
        "value" => value,
        "start_time" => Otel.nano_to_iso8601(data_point.start_time_unix_nano),
        "attributes" => Otel.handle_attributes(data_point.attributes),
        "timestamp" => Otel.nano_to_iso8601(data_point.time_unix_nano)
      })
    end)
  end

  defp handle_metric_data({:histogram, histogram}, base) do
    base =
      Map.merge(base, %{
        "metric_type" => "histogram",
        "aggregation_temporality" => aggregation_temporality(histogram.aggregation_temporality)
      })

    Enum.map(histogram.data_points, fn data_point ->
      Map.merge(base, %{
        "count" => data_point.count,
        "sum" => data_point.sum,
        "min" => data_point.min,
        "max" => data_point.max,
        "bucket_counts" => data_point.bucket_counts,
        "explicit_bounds" => data_point.explicit_bounds,
        "start_time" => Otel.nano_to_iso8601(data_point.start_time_unix_nano),
        "attributes" => Otel.handle_attributes(data_point.attributes),
        "timestamp" => Otel.nano_to_iso8601(data_point.time_unix_nano)
      })
    end)
  end

  defp handle_metric_data({:exponential_histogram, histogram}, base) do
    base =
      Map.merge(base, %{
        "metric_type" => "exponential_histogram",
        "aggregation_temporality" => aggregation_temporality(histogram.aggregation_temporality)
      })

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
        "start_time" => Otel.nano_to_iso8601(data_point.start_time_unix_nano),
        "attributes" => Otel.handle_attributes(data_point.attributes),
        "timestamp" => Otel.nano_to_iso8601(data_point.time_unix_nano)
      })
    end)
  end

  defp handle_metric_data({type, _}, _) do
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
