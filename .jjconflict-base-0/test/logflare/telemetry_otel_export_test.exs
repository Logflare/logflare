defmodule Logflare.TelemetryOtelExportTest do
  use ExUnit.Case, async: false

  import Telemetry.Metrics, only: [counter: 1]

  alias OtelMetricExporter.MetricStore

  describe "OTel resource export" do
    test "emits the build commit SHA as a service resource attribute" do
      resource = export_resource("deadbeef")

      assert resource["service.name"] == "Logflare"
      assert resource["service.commit"] == "deadbeef"
    end

    test "omits the commit attribute when the SHA is an empty string" do
      # An unset Docker build-arg expands `ENV LOGFLARE_COMMIT_SHA=${COMMIT_SHA}`
      # to "", so the var is present but empty in real deployments.
      resource = export_resource("")

      assert resource["service.name"] == "Logflare"
      refute Map.has_key?(resource, "service.commit")
    end

    test "omits the commit attribute when no SHA is set" do
      resource = export_resource(nil)

      assert resource["service.name"] == "Logflare"
      refute Map.has_key?(resource, "service.commit")
    end
  end

  # Builds the OTel resource via Telemetry.resource/0, runs it through a real
  # exporter, and returns the resource as it would be emitted downstream.
  defp export_resource(commit_sha) do
    test_pid = self()

    if commit_sha do
      System.put_env("LOGFLARE_COMMIT_SHA", commit_sha)
    else
      System.delete_env("LOGFLARE_COMMIT_SHA")
    end

    on_exit(fn -> System.delete_env("LOGFLARE_COMMIT_SHA") end)

    name = :"telemetry_otel_export_#{System.unique_integer([:positive])}"

    start_supervised!(
      {OtelMetricExporter,
       name: name,
       metrics: [counter("#{name}.count")],
       export_period: to_timeout(minute: 5),
       export_callback: fn {type, _batch}, config ->
         send(test_pid, {:otel_export, type, config.resource})
         :ok
       end,
       resource: Logflare.Telemetry.resource()}
    )

    assert :ok = MetricStore.export_sync(name)
    assert_receive {:otel_export, :metrics, resource}

    resource
  end
end
