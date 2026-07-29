defmodule LogflareGrpc.Health.ServerTest do
  use Logflare.DataCase, async: false

  alias Grpc.Health.V1.Health.Stub, as: HealthStub
  alias Grpc.Health.V1.HealthCheckRequest
  alias Grpc.Health.V1.HealthCheckResponse
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Opentelemetry.Proto.Collector.Metrics.V1.MetricsService.Stub, as: MetricsStub

  setup do
    insert(:plan)
    start_supervised!(AllLogsLogged)
    start_supervised!(GRPC.Client.Supervisor)

    {:ok, _pid, port} = GRPC.Server.start_endpoint(LogflareGrpc.Endpoint, 0)
    on_exit(fn -> GRPC.Server.stop_endpoint(LogflareGrpc.Endpoint) end)

    {:ok, %{port: port}}
  end

  test "check/2 returns SERVING without any credentials", %{port: port} do
    {:ok, channel} = GRPC.Stub.connect("localhost:#{port}")

    assert {:ok, %HealthCheckResponse{status: :SERVING}} =
             HealthStub.check(channel, %HealthCheckRequest{})
  end

  test "the OTLP servers still reject unauthenticated calls", %{port: port} do
    {:ok, channel} = GRPC.Stub.connect("localhost:#{port}")

    request = TestUtilsGrpc.random_otel_metrics_request()

    assert {:error, %GRPC.RPCError{status: 16}} = MetricsStub.export(channel, request)
  end
end
