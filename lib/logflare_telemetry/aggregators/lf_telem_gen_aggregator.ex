defmodule LogflareTelemetry.Aggregators.GenAggregator do
  alias LogflareTelemetry, as: LT
  alias LT.MetricsCache
  alias LT.Transformer

  def dispatch(metric, value, config) do
    backend = config.backend

    if measurement_exists?(value) do
      :ok =
        metric
        |> Transformer.event_to_payload(value, config)
        |> List.wrap()
        # |> transform_to_logs_ingest_dispatch()
        |> backend.ingest()
    end

    MetricsCache.reset(metric)
  end

  def measurement_exists?(nil), do: false
  def measurement_exists?([]), do: false
  def measurement_exists?(_), do: true

  def transform_to_logs_ingest_dispatch(values) do
    for value <- values do
      message =
        value
        |> Map.to_list()
        |> hd
        |> elem(0)

      message =
        message
        |> String.replace("logflare.", "")

      %{
        "metadata" => value,
        "message" => message
      }
    end
  end
end
