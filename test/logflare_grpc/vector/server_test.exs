defmodule LogflareGrpc.Vector.ServerTest do
  use Logflare.DataCase, async: false

  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.TestUtilsGrpc
  alias Vector.PushEventsRequest
  alias Vector.PushEventsResponse
  alias Vector.HealthCheckRequest
  alias Vector.HealthCheckResponse
  alias Vector.Vector.Stub

  setup do
    insert(:plan)
    start_supervised!(AllLogsLogged)
    start_supervised!(GRPC.Client.Supervisor)

    user = insert(:user)
    source = insert(:source, user: user)

    {:ok, _pid, port} = GRPC.Server.start_endpoint(LogflareGrpc.Endpoint, 0)
    on_exit(fn -> GRPC.Server.stop_endpoint(LogflareGrpc.Endpoint) end)
    {:ok, %{source: source, user: user, port: port}}
  end

  describe "push_events/2" do
    test "ingests Vector Log/Metric/Trace events", %{source: source, user: user, port: port} do
      access_token = insert(:access_token, resource_owner: user, scopes: "ingest")
      headers = [{"x-api-key", access_token.token}, {"x-source", source.token}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}",
          headers: headers,
          accepted_compressors: [GRPC.Compressor.Gzip]
        )

      request = TestUtilsGrpc.random_vector_push_events_request()
      assert {:ok, %PushEventsResponse{}} = Stub.push_events(channel, request)
    end

    test "rejects requests with missing source header", %{user: user, port: port} do
      access_token = insert(:access_token, resource_owner: user, scopes: "ingest")
      headers = [{"x-api-key", access_token.token}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}", headers: headers)

      assert {:error, %GRPC.RPCError{status: 16}} =
               Stub.push_events(channel, %PushEventsRequest{events: []})
    end

    test "rejects requests with missing api key", %{source: source, port: port} do
      headers = [{"x-source", source.token}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}", headers: headers)

      assert {:error, %GRPC.RPCError{status: 16}} =
               Stub.push_events(channel, %PushEventsRequest{events: []})
    end
  end

  describe "health_check/2" do
    test "returns SERVING", %{source: source, user: user, port: port} do
      access_token = insert(:access_token, resource_owner: user, scopes: "ingest")
      headers = [{"x-api-key", access_token.token}, {"x-source", source.token}]

      {:ok, channel} = GRPC.Stub.connect("localhost:#{port}", headers: headers)

      assert {:ok, %HealthCheckResponse{status: :SERVING}} =
               Stub.health_check(channel, %HealthCheckRequest{})
    end
  end
end
