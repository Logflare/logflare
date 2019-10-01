defmodule LogflareTelemetry.Transformer do
  @moduledoc """
  Transforms telemetry metrics and events to payloads dispatched to local and/or http backends.
  """

  alias Telemetry.Metrics.{Counter, Distribution, LastValue, Sum, Summary}
  alias LogflareTelemetry, as: LT
  alias LT.ExtendedMetrics, as: ExtMetrics
  alias LT.Enricher
  @default_schema_type :nested

  def event_to_payload(telem_metric, value, %{schema_type: :nested}) do
    metric = telem_metric.name ++ [metric_to_type(telem_metric)]
    metric = clean_metric(metric)

    metadata =
      metric
      |> Enum.reverse()
      |> Enum.reduce(value, fn
        key, acc -> %{key => acc}
      end)
      |> MapKeys.to_strings()

    metadata =
      Map.merge(
        metadata,
        %{
          "context" => %{
            "beam" => Enricher.beam_context()
          }
        }
      )

    %{
      "metadata" => metadata,
      "message" => Enum.join(metric, ".")
    }
  end

  def event_to_payload(telem_metric, value, %{schema_type: :flat}) do
    metric = telem_metric.name ++ [metric_to_type(telem_metric)]
    key = Enum.join(metric, ".")
    val = MapKeys.to_strings(value)

    Iteraptor.to_flatmap(%{key => val})
  end

  def event_to_payload(metric, val, config) do
    config = Map.put_new(config, :schema_type, @default_schema_type)

    case metric do
      %ExtMetrics.Every{} ->
        for value <- val do
          event_to_payload(metric, value, config)
        end

      %Summary{} ->
        val = prepare_summary_payload(val)
        event_to_payload(metric, val, config)

      _ ->
        event_to_payload(metric, val, config)
    end
  end

  def clean_metric([first | rest] = metric) do
    if first === :logflare do
      rest
    else
      metric
    end
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
  def metric_to_type(%ExtMetrics.LastValues{}), do: :last_values
  def metric_to_type(%ExtMetrics.Every{}), do: :every
end
