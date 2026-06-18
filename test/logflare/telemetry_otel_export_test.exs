defmodule Logflare.TelemetryOtelExportTest do
  use ExUnit.Case, async: false

  import Telemetry.Metrics, only: [counter: 1]

  alias OtelMetricExporter.MetricStore

  describe "OTel resource export" do
    test "emits the build commit SHA as a service resource attribute" do
      test_pid = self()

      System.put_env("LOGFLARE_COMMIT_SHA", "deadbeef")
      on_exit(fn -> System.delete_env("LOGFLARE_COMMIT_SHA") end)

      name = :telemetry_otel_export_test

      start_supervised!(
        {OtelMetricExporter,
         name: name,
         metrics: [counter("logflare.telemetry_otel_export_test.count")],
         export_period: to_timeout(minute: 5),
         export_callback: fn {type, _batch}, config ->
           send(test_pid, {:otel_export, type, config.resource})
           :ok
         end,
         resource: Logflare.Telemetry.resource()}
      )

      assert :ok = MetricStore.export_sync(name)

      assert_receive {:otel_export, :metrics, resource}
      assert resource["service.name"] == "Logflare"
      assert resource["service.commit"] == "deadbeef"
    end

    test "omits the commit attribute when no SHA is set" do
      test_pid = self()

      System.delete_env("LOGFLARE_COMMIT_SHA")

      name = :telemetry_otel_export_no_sha_test

      start_supervised!(
        {OtelMetricExporter,
         name: name,
         metrics: [counter("logflare.telemetry_otel_export_no_sha_test.count")],
         export_period: to_timeout(minute: 5),
         export_callback: fn {type, _batch}, config ->
           send(test_pid, {:otel_export, type, config.resource})
           :ok
         end,
         resource: Logflare.Telemetry.resource()}
      )

      assert :ok = MetricStore.export_sync(name)

      assert_receive {:otel_export, :metrics, resource}
      assert resource["service.name"] == "Logflare"
      refute Map.has_key?(resource, "service.commit")
    end
  end
end
