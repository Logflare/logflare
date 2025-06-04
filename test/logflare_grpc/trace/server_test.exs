defmodule LogflareGrpc.Trace.ServerTest do
  use Logflare.DataCase, async: false

  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceResponse
  alias Opentelemetry.Proto.Collector.Trace.V1.TraceService.Stub
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

    defp emulate_request(channel, request) do
      Stub.export(channel, request)
      # GRPC.Stub.send_request(stream, request)
      # GRPC.Stub.disconnect(channel)
      # GRPC.Stub.end_stream(stream)
      # reply
    end

    test "returns a success response and starts log event ingestion using access token", %{
      source: source,
      user: user,
      port: port
    } do
      access_token = insert(:access_token, resource_owner: user)
      headers = [{"x-api-key", access_token.token}, {"x-source", source.token}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}",
          headers: headers,
          accepted_compressors: [GRPC.Compressor.Gzip]
        )

      request = TestUtilsGrpc.random_export_service_request()

      assert {:ok, %ExportTraceServiceResponse{}} =
               channel
               |> emulate_request(request)
    end

    test "returns a success using access token for specific source", %{
      source: source,
      user: user,
      port: port
    } do
      access_token =
        insert(:access_token, resource_owner: user, scopes: "ingest ingest:source:#{source.id}")

      headers = [{"x-api-key", access_token.token}, {"x-source", source.token}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}",
          headers: headers,
          accepted_compressors: [GRPC.Compressor.Gzip]
        )

      request = TestUtilsGrpc.random_export_service_request()

      assert {:ok, %ExportTraceServiceResponse{}} =
               channel
               |> emulate_request(request)
    end

    test "returns a success response and starts log event ingestion using legacy api key", %{
      source: source,
      user: user,
      port: port
    } do
      headers = [{"x-api-key", user.api_key}, {"x-source", source.token}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}",
          headers: headers,
          accepted_compressors: [GRPC.Compressor.Gzip]
        )

      request = TestUtilsGrpc.random_export_service_request()

      assert {:ok, %ExportTraceServiceResponse{}} =
               channel
               |> emulate_request(request)
    end

    test "returns an error if invalid api key", %{source: source, port: port} do
      headers = [{"x-api-key", "potato"}, {"x-source", source.token}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}",
          headers: headers,
          accepted_compressors: [GRPC.Compressor.Gzip]
        )

      request = TestUtilsGrpc.random_export_service_request()

      {:error, err} = emulate_request(channel, request)

      assert %GRPC.RPCError{message: "Invalid API key"} = err
    end

    test "returns an error using invalid access token for specific source", %{
      source: source,
      user: user,
      port: port
    } do
      access_token =
        insert(:access_token, resource_owner: user, scopes: "ingest:source:#{source.id + 155}")

      headers = [{"x-api-key", access_token.token}, {"x-source", source.token}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}",
          headers: headers,
          accepted_compressors: [GRPC.Compressor.Gzip]
        )

      request = TestUtilsGrpc.random_export_service_request()

      {:error, err} = emulate_request(channel, request)

      assert %GRPC.RPCError{message: "Invalid API key"} = err
    end

    test "returns an error if invalid source ID", %{
      user: user,
      port: port
    } do
      headers = [{"x-api-key", user.api_key}, {"x-source", "potato"}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}",
          headers: headers,
          accepted_compressors: [GRPC.Compressor.Gzip]
        )

      request = TestUtilsGrpc.random_export_service_request()

      {:error, result} = emulate_request(channel, request)

      assert %GRPC.RPCError{message: "Invalid source id"} = result
    end

    test "returns an error if missing x-api-key header", %{source: source, port: port} do
      headers = [{"x-source", source.token}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}",
          headers: headers,
          accepted_compressors: [GRPC.Compressor.Gzip]
        )

      request = TestUtilsGrpc.random_export_service_request()

      {:error, result} = emulate_request(channel, request)

      assert %GRPC.RPCError{message: "Missing or invalid API key"} = result
    end

    test "returns an error if missing x-source header", %{
      user: user,
      port: port
    } do
      headers = [{"x-api-key", user.api_key}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}",
          headers: headers,
          accepted_compressors: [GRPC.Compressor.Gzip]
        )

      request = TestUtilsGrpc.random_export_service_request()

      {:error, result} = emulate_request(channel, request)

      assert %GRPC.RPCError{message: "Missing or invalid source id"} = result
    end
  end
end
