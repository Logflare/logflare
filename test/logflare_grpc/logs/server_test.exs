defmodule LogflareGrpc.Logs.ServerTest do
  use Logflare.DataCase, async: false

  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse
  alias Opentelemetry.Proto.Collector.Logs.V1.LogsService.Stub
  alias Logflare.SystemMetrics.AllLogsLogged

  setup do
    insert(:plan)
    start_supervised!(AllLogsLogged)

    :ok
  end

  describe "export/2" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      {:ok, _pid, port} = GRPC.Server.start_endpoint(LogflareGrpc.Endpoint, 0)
      on_exit(fn -> GRPC.Server.stop_endpoint(LogflareGrpc.Endpoint) end)
      {:ok, %{source: source, user: user, port: port}}
    end

    test "returns a success response and starts log event ingestion using access token", %{
      source: source,
      user: user,
      port: port
    } do
      access_token = insert(:access_token, resource_owner: user, scopes: "ingest")
      headers = [{"x-api-key", access_token.token}, {"x-source", source.token}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}",
          headers: headers,
          accepted_compressors: [GRPC.Compressor.Gzip]
        )

      request = TestUtilsGrpc.random_otel_logs_request()

      assert {:ok, %ExportLogsServiceResponse{}} = emulate_request(channel, request)
    end
  end

  defp emulate_request(channel, request) do
    Stub.export(channel, request)
  end
end
