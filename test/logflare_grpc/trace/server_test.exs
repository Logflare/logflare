defmodule LogflareGrpc.Trace.ServerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Logs
  alias LogflareGrpc.Trace.Server
  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceResponse
  alias Opentelemetry.Proto.Collector.Trace.V1.TraceService.Stub

  @port 50051

  describe "export/2" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      {:ok, _pid, _port} = GRPC.Server.start([Server], @port)
      on_exit(fn -> GRPC.Server.stop([Server]) end)
      {:ok, %{source: source, user: user}}
    end

    test "returns a success response and starts log event ingestion", %{
      source: source,
      user: user
    } do
      headers = [{"x-api-key", user.api_key}, {"x-source-id", source.token}]
      {:ok, channel} = GRPC.Stub.connect("localhost:#{@port}", headers: headers)

      request = TestUtilsGrpc.random_export_service_request()

      expect(Logs, :ingest, 2, fn _ -> :ok end)

      assert {:ok, %ExportTraceServiceResponse{}} = Stub.export(channel, request)
    end

    test "returns an error if invalid api key", %{source: source} do
      headers = [{"x-api-key", "potato"}, {"x-source-id", source.token}]
      {:ok, channel} = GRPC.Stub.connect("localhost:#{@port}", headers: headers)

      request = TestUtilsGrpc.random_export_service_request()
      reject(Logs, :ingest, 1)

      assert {:error, %GRPC.RPCError{message: "Invalid API Key or Source ID"}} =
               Stub.export(channel, request)
    end

    test "returns an error if invalid source ID", %{
      user: user
    } do
      headers = [{"x-api-key", user.api_key}, {"x-source-id", "potato"}]
      {:ok, channel} = GRPC.Stub.connect("localhost:#{@port}", headers: headers)

      request = TestUtilsGrpc.random_export_service_request()
      reject(Logs, :ingest, 1)

      assert {:error, %GRPC.RPCError{message: "Invalid API Key or Source ID"}} =
               Stub.export(channel, request)
    end

    test "returns an error if missing x-api-key header", %{source: source} do
      headers = [{"x-source-id", source.token}]
      {:ok, channel} = GRPC.Stub.connect("localhost:#{@port}", headers: headers)

      request = TestUtilsGrpc.random_export_service_request()
      reject(Logs, :ingest, 1)

      assert {:error, %GRPC.RPCError{message: "Invalid API Key or Source ID"}} =
               Stub.export(channel, request)
    end

    test "returns an error if missing x-source-id header", %{
      user: user
    } do
      headers = [{"x-api-key", user.api_key}]
      {:ok, channel} = GRPC.Stub.connect("localhost:#{@port}", headers: headers)

      request = TestUtilsGrpc.random_export_service_request()
      reject(Logs, :ingest, 1)

      assert {:error, %GRPC.RPCError{message: "Invalid API Key or Source ID"}} =
               Stub.export(channel, request)
    end
  end
end
