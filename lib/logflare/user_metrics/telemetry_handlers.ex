defmodule Logflare.UserMetrics.TelemetryHandlers do
  @moduledoc false

  # Attaches telemetry handlers after MetricStore has started (rest_for_one ordering).
  # Detaches on shutdown to prevent dangling handlers.

  use GenServer

  alias Logflare.UserMetrics.MetricStore

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: __MODULE__)
  end

  @impl true
  def init(config) do
    Process.flag(:trap_exit, true)
    handlers = setup_handlers(config)
    {:ok, %{handlers: handlers}, :hibernate}
  end

  @impl true
  def terminate(_reason, %{handlers: handlers}) do
    for handler_id <- handlers, do: :telemetry.detach(handler_id)
  end

  defp setup_handlers(%{metrics: metrics, store_name: store_name, extract_tags: extract_tags_fn}) do
    metrics
    |> Enum.group_by(& &1.event_name)
    |> Enum.map(fn {event_name, metrics_for_event} ->
      handler_id = {__MODULE__, event_name}

      :telemetry.attach(
        handler_id,
        event_name,
        &__MODULE__.handle_metric/4,
        %{metrics: metrics_for_event, store_name: store_name, extract_tags: extract_tags_fn}
      )

      handler_id
    end)
  end

  @doc false
  def handle_metric(_event_name, measurements, metadata, config) do
    %{metrics: metrics, store_name: store_name, extract_tags: extract_tags_fn} = config

    for metric <- metrics do
      if is_nil(metric.keep) || metric.keep.(metadata) do
        value = extract_measurement(metric, measurements, metadata)
        tags = extract_tags_fn.(metric, metadata)
        metric_name = Enum.join(metric.name, ".")
        MetricStore.write_metric(store_name, metric, metric_name, value, tags)
      end
    end
  end

  defp extract_measurement(metric, measurements, metadata) do
    case metric.measurement do
      fun when is_function(fun, 1) -> fun.(measurements)
      fun when is_function(fun, 2) -> fun.(measurements, metadata)
      key -> Map.get(measurements, key)
    end
  end
end
