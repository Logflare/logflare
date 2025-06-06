defmodule LogflareGrpc.Interceptors.VerifyApiResourceAccessTest do
  use Logflare.DataCase, async: false

  alias Logflare.TestProtobuf.Mock.EmptyRequest
  alias Logflare.TestProtobuf.Mock.EmptyResponse
  alias Logflare.SystemMetrics.AllLogsLogged

  defmodule MockServer do
    use GRPC.Server, service: Logflare.TestProtobuf.Mock.MockService.Service

    def do_nothing(_, _request) do
      %EmptyResponse{}
    end
  end

  defmodule TestEndpoint do
    use GRPC.Endpoint

    intercept(LogflareGrpc.Interceptors.VerifyApiResourceAccess)
    run(MockServer)
  end

  setup do
    insert(:plan)
    start_supervised!(AllLogsLogged)

    :ok
  end

  describe "call/2" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      {:ok, _pid, port} = GRPC.Server.start_endpoint(TestEndpoint, 0)
      on_exit(fn -> GRPC.Server.stop_endpoint(TestEndpoint) end)
      {:ok, %{source: source, user: user, port: port}}
    end

    test "success response when both headers are valid", %{
      source: source,
      user: user,
      port: port
    } do
      access_token = insert(:access_token, resource_owner: user)
      headers = [{"x-api-key", access_token.token}, {"x-source", source.token}]

      assert {:ok, %EmptyResponse{}} = request_with_headers(headers, port)
    end

    test "success using access token for specific source", %{
      source: source,
      user: user,
      port: port
    } do
      access_token =
        insert(:access_token, resource_owner: user, scopes: "ingest ingest:source:#{source.id}")

      headers = [{"x-api-key", access_token.token}, {"x-source", source.token}]

      assert {:ok, %EmptyResponse{}} = request_with_headers(headers, port)
    end

    test "success using legacy api key", %{
      source: source,
      user: user,
      port: port
    } do
      headers = [{"x-api-key", user.api_key}, {"x-source", source.token}]

      assert {:ok, %EmptyResponse{}} = request_with_headers(headers, port)
    end

    test "returns an error if invalid api key", %{source: source, port: port} do
      headers = [{"x-api-key", "potato"}, {"x-source", source.token}]

      # Permission denied
      assert {:error, %GRPC.RPCError{status: 7}} = request_with_headers(headers, port)
    end

    test "returns an error using invalid access token for specific source", %{
      source: source,
      user: user,
      port: port
    } do
      access_token =
        insert(:access_token, resource_owner: user, scopes: "ingest:source:#{source.id + 155}")

      headers = [{"x-api-key", access_token.token}, {"x-source", source.token}]

      # Permission denied
      assert {:error, %GRPC.RPCError{status: 7}} = request_with_headers(headers, port)
    end

    test "returns an error if invalid source ID", %{
      user: user,
      port: port
    } do
      headers = [{"x-api-key", user.api_key}, {"x-source", "potato"}]

      assert {:error, %GRPC.RPCError{message: "Invalid source id"}} =
               request_with_headers(headers, port)
    end

    test "returns an error if missing x-api-key header", %{source: source, port: port} do
      headers = [{"x-source", source.token}]

      assert {:error, %GRPC.RPCError{message: "Missing or invalid API key"}} =
               request_with_headers(headers, port)
    end

    test "returns an error if missing x-source header", %{
      user: user,
      port: port
    } do
      headers = [{"x-api-key", user.api_key}]

      assert {:error, %GRPC.RPCError{message: "Missing source id"}} =
               request_with_headers(headers, port)
    end
  end

  defp request_with_headers(headers, port) do
    {:ok, channel} =
      GRPC.Stub.connect("localhost:#{port}",
        headers: headers,
        accepted_compressors: [GRPC.Compressor.Gzip]
      )

    Logflare.TestProtobuf.Mock.MockService.Stub.do_nothing(
      channel,
      %EmptyRequest{}
    )
  end
end
