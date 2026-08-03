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
    |> Enum.flat_map(&handle_resource_metrics(&1))
  end

  def handle_resource_metrics(%_resource_metrics{
        resource: resource,
        scope_metrics: scope_metrics
      }) do
    resource = Otel.handle_resource(resource)

    Enum.flat_map(scope_metrics, fn scope_metric ->
      scope = Otel.handle_scope(scope_metric.scope)

      Enum.flat_map(scope_metric.metrics, fn metric ->
        handle_metric(metric, resource, scope)
      end)
    end)
  end

  def handle_metric(%_metric{name: name, unit: unit, data: data}, resource, scope) do
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
    event_message = base["event_message"]
    unit = base["unit"]
    metadata = base["metadata"]
    scope = base["scope"]
    resource = base["resource"]
    project = base["project"]

    for data_point <- data_points do
      %{value: {_, value}} = data_point

      %{
        "event_message" => event_message,
        "unit" => unit,
        "metadata" => metadata,
        "scope" => scope,
        "resource" => resource,
        "project" => project,
        "metric_type" => "gauge",
        "value" => value,
        "start_time" => data_point.start_time_unix_nano,
        "attributes" => Otel.handle_attributes(data_point.attributes),
        "timestamp" => data_point.time_unix_nano
      }
    end
  end

  defp handle_metric_data({:sum, sum}, base) do
    event_message = base["event_message"]
    unit = base["unit"]
    metadata = base["metadata"]
    scope = base["scope"]
    resource = base["resource"]
    project = base["project"]
    temporality = aggregation_temporality(sum.aggregation_temporality)
    is_monotonic = sum.is_monotonic

    for data_point <- sum.data_points do
      %{value: {_, value}} = data_point

      %{
        "event_message" => event_message,
        "unit" => unit,
        "metadata" => metadata,
        "scope" => scope,
        "resource" => resource,
        "project" => project,
        "metric_type" => "sum",
        "aggregation_temporality" => temporality,
        "is_monotonic" => is_monotonic,
        "value" => value,
        "start_time" => data_point.start_time_unix_nano,
        "attributes" => Otel.handle_attributes(data_point.attributes),
        "timestamp" => data_point.time_unix_nano
      }
    end
  end

  defp handle_metric_data({:histogram, histogram}, base) do
    event_message = base["event_message"]
    unit = base["unit"]
    metadata = base["metadata"]
    scope = base["scope"]
    resource = base["resource"]
    project = base["project"]
    temporality = aggregation_temporality(histogram.aggregation_temporality)

    for data_point <- histogram.data_points do
      %{
        "event_message" => event_message,
        "unit" => unit,
        "metadata" => metadata,
        "scope" => scope,
        "resource" => resource,
        "project" => project,
        "metric_type" => "histogram",
        "aggregation_temporality" => temporality,
        "count" => data_point.count,
        "sum" => data_point.sum,
        "min" => data_point.min,
        "max" => data_point.max,
        "bucket_counts" => data_point.bucket_counts,
        "explicit_bounds" => data_point.explicit_bounds,
        "start_time" => data_point.start_time_unix_nano,
        "attributes" => Otel.handle_attributes(data_point.attributes),
        "timestamp" => data_point.time_unix_nano
      }
    end
  end

  defp handle_metric_data({:exponential_histogram, histogram}, base) do
    event_message = base["event_message"]
    unit = base["unit"]
    metadata = base["metadata"]
    scope = base["scope"]
    resource = base["resource"]
    project = base["project"]
    temporality = aggregation_temporality(histogram.aggregation_temporality)

    for data_point <- histogram.data_points do
      %{
        "event_message" => event_message,
        "unit" => unit,
        "metadata" => metadata,
        "scope" => scope,
        "resource" => resource,
        "project" => project,
        "metric_type" => "exponential_histogram",
        "aggregation_temporality" => temporality,
        "count" => data_point.count,
        "sum" => data_point.sum,
        "scale" => data_point.scale,
        "zero_count" => data_point.scale,
        "zero_threshold" => data_point.zero_threshold,
        "positive" => exponential_histogram_buckets(data_point.positive),
        "negative" => exponential_histogram_buckets(data_point.negative),
        "min" => data_point.min,
        "max" => data_point.max,
        "start_time" => data_point.start_time_unix_nano,
        "attributes" => Otel.handle_attributes(data_point.attributes),
        "timestamp" => data_point.time_unix_nano
      }
    end
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
