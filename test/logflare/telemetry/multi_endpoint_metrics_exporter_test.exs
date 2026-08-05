defmodule Logflare.Telemetry.MultiEndpointMetricsExporterTest do
  use ExUnit.Case, async: true
  use Mimic

  alias Logflare.Telemetry.MultiEndpointMetricsExporter
  alias OtelMetricExporter.OtelApi.Config

  @config %Config{
    otlp_endpoint: "https://primary.example.com",
    otlp_headers: %{"x-api-key" => "primary-key"},
    resource: %{"service.name" => "logflare"}
  }

  describe "export_metrics/3" do
    test "posts the same batch to the primary endpoint and every extra endpoint" do
      extra_endpoints = [
        %{
          otlp_endpoint: "https://secondary.example.com",
          otlp_headers: %{"x-api-key" => "secondary-key"}
        }
      ]

      this = self()

      expect(Finch, :request, 2, fn request, OtelMetricExporter.Finch ->
        send(this, {:request, request})
        {:ok, %Finch.Response{status: 200, body: "", headers: []}}
      end)

      assert :ok =
               MultiEndpointMetricsExporter.export_metrics(
                 {:metrics, []},
                 @config,
                 extra_endpoints
               )

      assert_receive {:request, %Finch.Request{host: "primary.example.com"} = primary_req}
      assert_receive {:request, %Finch.Request{host: "secondary.example.com"} = secondary_req}

      assert primary_req.path == "/v1/metrics"
      assert secondary_req.path == "/v1/metrics"
      assert {"x-api-key", "primary-key"} in primary_req.headers
      assert {"x-api-key", "secondary-key"} in secondary_req.headers
      assert {"content-encoding", "gzip"} in primary_req.headers
      assert {"content-encoding", "gzip"} in secondary_req.headers
    end

    test "returns :ok and only hits the primary endpoint when there are no extra endpoints" do
      expect(Finch, :request, fn %Finch.Request{host: "primary.example.com"}, _pool ->
        {:ok, %Finch.Response{status: 202, body: "", headers: []}}
      end)

      assert :ok = MultiEndpointMetricsExporter.export_metrics({:metrics, []}, @config, [])
    end

    test "fails the whole batch when any endpoint fails, identifying which one" do
      extra_endpoints = [%{otlp_endpoint: "https://secondary.example.com", otlp_headers: %{}}]

      expect(Finch, :request, 2, fn
        %Finch.Request{host: "primary.example.com"}, _pool ->
          {:ok, %Finch.Response{status: 200, body: "", headers: []}}

        %Finch.Request{host: "secondary.example.com"}, _pool ->
          {:ok, %Finch.Response{status: 500, body: "boom", headers: []}}
      end)

      assert {:error, errors} =
               MultiEndpointMetricsExporter.export_metrics(
                 {:metrics, []},
                 @config,
                 extra_endpoints
               )

      assert [
               {:error,
                {"https://secondary.example.com", {:unexpected_status, %Finch.Response{}}}}
             ] = errors
    end

    test "reports a transport error against the offending endpoint" do
      extra_endpoints = [%{otlp_endpoint: "https://secondary.example.com", otlp_headers: %{}}]

      expect(Finch, :request, 2, fn
        %Finch.Request{host: "primary.example.com"}, _pool ->
          {:ok, %Finch.Response{status: 200, body: "", headers: []}}

        %Finch.Request{host: "secondary.example.com"}, _pool ->
          {:error, %Mint.TransportError{reason: :econnrefused}}
      end)

      assert {:error, [{:error, {"https://secondary.example.com", %Mint.TransportError{}}}]} =
               MultiEndpointMetricsExporter.export_metrics(
                 {:metrics, []},
                 @config,
                 extra_endpoints
               )
    end
  end
end
