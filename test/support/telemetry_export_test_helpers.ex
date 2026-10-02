defmodule Logflare.TelemetryExportTestHelpers do
  import ExUnit.Assertions
  import ExUnit.Callbacks

  alias OtelMetricExporter.MetricStore

  @spec start_exporter([struct()], atom()) :: atom()
  def start_exporter(metrics, name) do
    test_pid = self()

    start_supervised!(
      {OtelMetricExporter,
       name: name,
       metrics: metrics,
       export_period: to_timeout(minute: 5),
       export_callback: fn {:metrics, batch}, _config ->
         send(test_pid, {name, batch})
         :ok
       end}
    )

    name
  end

  @spec export_metrics(atom()) :: [map()]
  def export_metrics(name) do
    assert :ok = MetricStore.export_sync(name)
    assert_receive {^name, batch}, 1_000
    batch
  end

  @spec point_attributes(map()) :: map()
  def point_attributes(%{attributes: attributes}) do
    Map.new(attributes, fn %{key: key, value: %{value: {_type, value}}} -> {key, value} end)
  end
end
