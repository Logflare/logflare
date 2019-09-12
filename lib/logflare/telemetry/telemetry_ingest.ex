defmodule Logflare.TelemetryBackend.BQ do
  @moduledoc false
  require Logger
  alias Logflare.Logs
  alias Logflare.Sources
  @default_source_id Application.get_env(:logflare_telemetry, :source_id)
  alias Telemetry.Metrics.{Counter, Distribution, LastValue, Sum, Summary}
  alias LogflareTelemetry, as: LT
  alias LT.LogflareMetrics, as: LMetrics

  def ingest(%LMetrics.All{} = metric, values) do
    source = Sources.Cache.get_by_id(@default_source_id)

    log_params_batch =
      for value <- values do
        metadata =
          metric.name
          |> Enum.reverse()
          |> Enum.reduce(value, fn
            key, acc -> %{key => acc}
          end)
          |> MapKeys.to_strings()

        %{"metadata" => metadata, "message" => Enum.join(metric.name, ".")}
      end

    Logs.ingest_logs(log_params_batch, source)

    :ok
  end

  def ingest(metric, value) do
    source = Sources.Cache.get_by_id(@default_source_id)

    value =
      case metric do
        %Summary{} -> prepare_summary_payload(value)
        _ -> value
      end

    metric_id = metric.name ++ [metric_to_type(metric)]

    metadata =
      metric_id
      |> Enum.reverse()
      |> Enum.reduce(value, fn
        key, acc ->
          %{key => acc}
      end)
      |> MapKeys.to_strings()

    Logs.ingest_logs([%{"metadata" => metadata, "message" => Enum.join(metric_id, ".")}], source)

    :ok
  end

  def prepare_summary_payload(payload) do
    for {k, v} <- payload do
      case k do
        :percentiles ->
          {Atom.to_string(k),
           v |> Enum.map(fn {k, v} -> {"percentile_#{k}", Float.round(v / 1000)} end) |> Map.new()}

        _ ->
          {Atom.to_string(k), Float.round(v / 1000)}
      end
    end
    |> Map.new()
  end

  def metric_to_type(%Summary{}), do: :summary
  def metric_to_type(%LastValue{}), do: :last_value
  def metric_to_type(%Counter{}), do: :counter
  def metric_to_type(%Distribution{}), do: :distribution
  def metric_to_type(%Sum{}), do: :sum
  def metric_to_type(%LMetrics.LastValues{}), do: :last_values
end
