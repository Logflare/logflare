defmodule Logflare.TelemetryBackend.BQ do
  @moduledoc false
  require Logger
  alias Logflare.Logs
  alias Logflare.Sources
  @default_source_id Application.get_env(:logflare_telemetry, :source_id)
  alias Telemetry.Metrics.{Counter, Distribution, LastValue, Sum, Summary}

  def ingest(metric, value) do
    source = Sources.Cache.get_by_id(@default_source_id)

    value =
      case metric do
        %Summary{} -> prepare_summary_payload(value)
        _ -> value
      end

    metadata =
      (metric.event_name ++ [metric.measurement] ++ [metric_to_type(metric)])
      |> Enum.reverse()
      |> Enum.reduce(value, fn
        key, acc when is_atom(key) ->
          %{Atom.to_string(key) => acc}

        key, acc ->
          %{key => acc}
      end)

    Logs.ingest_logs([%{"metadata" => metadata, "message" => "telemetry"}], source)

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
end
