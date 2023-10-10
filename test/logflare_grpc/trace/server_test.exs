defmodule LogflareGrpc.Trace.ServerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Logs
  alias LogflareGrpc.Trace.Server
  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceResponse
  alias Opentelemetry.Proto.Collector.Trace.V1.TraceService.Stub

  describe "export/2" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      {:ok, _pid, port} = GRPC.Server.start([Server], 0)
      on_exit(fn -> GRPC.Server.stop([Server]) end)
      {:ok, %{source: source, user: user, port: port}}
    end

    defp emulate_request(channel, request) do
      stream = Stub.export(channel)
      GRPC.Stub.send_request(stream, request)
      GRPC.Stub.disconnect(channel)
      GRPC.Stub.end_stream(stream)
    end

    test "returns a success response and starts log event ingestion", %{
      source: source,
      user: user,
      port: port
    } do
      headers = [{"x-api-key", user.api_key}, {"x-source-id", source.token}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}",
          headers: headers,
          accepted_compressors: [GRPC.Compressor.Gzip]
        )

      request = TestUtilsGrpc.random_export_service_request()
      expect(Logs, :ingest, 2, fn _ -> :ok end)

      result =
        channel
        |> emulate_request(request)
        |> GRPC.Stub.recv(timeout: 100, return_headers: true)
        |> then(&elem(&1, 1))
        |> Enum.to_list()

      assert [
               {:ok, %ExportTraceServiceResponse{}},
               {:ok, %ExportTraceServiceResponse{}},
               {:trailers, %{"grpc-message" => "", "grpc-status" => "0"}}
             ] = result
    end

    test "returns an error if invalid api key", %{source: source, port: port} do
      headers = [{"x-api-key", "potato"}, {"x-source-id", source.token}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}",
          headers: headers,
          accepted_compressors: [GRPC.Compressor.Gzip]
        )

      request = TestUtilsGrpc.random_export_service_request()
      reject(Logs, :ingest, 1)

      result =
        channel
        |> emulate_request(request)
        |> GRPC.Stub.recv(timeout: 100, return_headers: true)
        |> then(&elem(&1, 1))

      assert %GRPC.RPCError{message: "Invalid API Key or Source ID"} = result
    end

    test "returns an error if invalid source ID", %{
      user: user,
      port: port
    } do
      headers = [{"x-api-key", user.api_key}, {"x-source-id", "potato"}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}",
          headers: headers,
          accepted_compressors: [GRPC.Compressor.Gzip]
        )

      request = TestUtilsGrpc.random_export_service_request()
      reject(Logs, :ingest, 1)

      result =
        channel
        |> emulate_request(request)
        |> GRPC.Stub.recv(timeout: 100, return_headers: true)
        |> then(&elem(&1, 1))

      assert %GRPC.RPCError{message: "Invalid API Key or Source ID"} = result
    end

    test "returns an error if missing x-api-key header", %{source: source, port: port} do
      headers = [{"x-source-id", source.token}]

      {:ok, channel} =
        GRPC.Stub.connect("localhost:#{port}",
          headers: headers,
          accepted_compressors: [GRPC.Compressor.Gzip]
        )

      request = TestUtilsGrpc.random_export_service_request()
      reject(Logs, :ingest, 1)

      result =
        channel
        |> emulate_request(request)
        |> GRPC.Stub.recv(timeout: 100, return_headers: true)
        |> then(&elem(&1, 1))

      assert %GRPC.RPCError{message: "Invalid API Key or Source ID"} = result
    end

    test "returns an error if missing x-source-id header", %{
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
      reject(Logs, :ingest, 1)

      result =
        channel
        |> emulate_request(request)
        |> GRPC.Stub.recv(timeout: 100, return_headers: true)
        |> then(&elem(&1, 1))

      assert %GRPC.RPCError{message: "Invalid API Key or Source ID"} = result
    end
  end
end
