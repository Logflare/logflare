defmodule Logflare.Logs.OtelMetric do
  @moduledoc """
  Converts a Otel metric to event parameters

  Converts a list of ResourceMetrics to a list of Logflare Events

  Important details about the conversation:
  * One ResourceMetric can contain multiple ScopeSpans
  * One ScopeMetrics can contain multiple Metrics
  * One Metric can contain multiple DataPoints
  * A Log Event is created for each DataPoint, including parent metric data
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
    metric_type = "gauge"

    Enum.map(data_points, fn data_point ->
      %{value: {_, value}} = data_point

      %{
        "event_message" => metric.name,
        "metadata" => metadata,
        "start_time" => Logflare.Logs.Otel.nano_to_iso8601(data_point.start_time_unix_nano),
        "unit" => metric.unit,
        "value" => value,
        "metric_type" => metric_type,
        "attributes" => Logflare.Logs.Otel.handle_attributes(data_point.attributes),
        "timestamp" => Logflare.Logs.Otel.nano_to_iso8601(data_point.time_unix_nano),
        "project" => metadata["name"]
      }
    end)
  end

  # TODO: support other metric types
  defp handle_metric_data(_, _, _), do: []
end
