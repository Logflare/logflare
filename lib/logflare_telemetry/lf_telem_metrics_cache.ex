defmodule LogflareTelemetry.MetricsCache do
  @moduledoc """
  Caches the telemetry metric measurements until aggregators process them.
  """
  @c :logflare_telemetry_measurement_cache
  alias Telemetry.Metrics.{Counter, Summary, Sum, LastValue, Distribution}
  alias LogflareTelemetry.ExtendedMetrics, as: ExtMetrics

  @type telemetry_metric :: Counter | Distribution | LastValue | Sum | Summary

  def child_spec(_) do
    %{
      id: :cachex_sources_cache,
      start: {
        Cachex,
        :start_link,
        [@c, []]
      }
    }
  end

  def get(metric) do
    Cachex.get(@c, metric_to_key(metric))
  end

  def increment(metric, amount \\ 1) do
    case Cachex.exists?(@c, metric) do
      {:ok, true} -> Cachex.incr(@c, metric_to_key(metric), amount)
      {:ok, false} -> Cachex.put(@c, metric_to_key(metric), amount)
    end
  end

  def put(metric, value) do
    Cachex.put(
      @c,
      metric_to_key(metric),
      value
    )
  end

  def reset(metric) do
    Cachex.put(
      @c,
      metric_to_key(metric),
      case metric do
        %Counter{} -> 0
        %Sum{} -> 0
        %Summary{} -> []
        %Distribution{} -> []
        %LastValue{} -> nil
        %ExtMetrics.Every{} -> []
        %ExtMetrics.LastValues{} -> []
      end
    )
  end

  def push(metric, measurement) do
    Cachex.get_and_update(@c, metric_to_key(metric), fn
      nil -> {:commit, [measurement]}
      xs -> {:commit, [measurement | xs]}
    end)
  end

  def cache_id(), do: @c

  defp metric_to_key(metric) do
    metric_type =
      case metric do
        %Summary{} -> :summary
        %Sum{} -> :sum
        %LastValue{} -> :last_value
        %Counter{} -> :counter
        %Distribution{} -> :distribution
        %ExtMetrics.Every{} -> []
        %ExtMetrics.LastValues{} -> []
      end

    metric.name ++ metric_type
  end
end
