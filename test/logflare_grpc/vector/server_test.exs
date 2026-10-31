defmodule LogflareGrpc.Vector.ServerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.TestUtilsGrpc
  alias Vector.HealthCheckRequest
  alias Vector.HealthCheckResponse
  alias Vector.PushEventsRequest
  alias Vector.PushEventsResponse
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

  describe "push_events/2 with postgres backend" do
    setup %{source: source} do
      repo = Application.get_env(:logflare, Logflare.Repo)

      url =
        "postgresql://#{repo[:username]}:#{repo[:password]}@#{repo[:hostname]}/#{repo[:database]}"

      backend =
        insert(:backend, type: :postgres, sources: [source], config: %{url: url, schema: nil})

      start_supervised!({AdaptorSupervisor, {source, backend}})
      on_exit(fn -> PostgresAdaptor.destroy_instance({source, backend}) end)

      {:ok, %{backend: backend}}
    end

    test "ingests a Vector log event end-to-end into postgres", %{
      backend: backend,
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

      message = "vector-grpc-#{Logflare.TestUtils.random_string()}"

      request = %PushEventsRequest{
        events: [TestUtilsGrpc.random_vector_log_event(message)]
      }

      assert {:ok, %PushEventsResponse{}} = Stub.push_events(channel, request)

      table = PostgresAdaptor.table_name(source)

      TestUtils.retry_assert(fn ->
        assert {:ok, %QueryResult{rows: rows, total_rows: total}} =
                 PostgresAdaptor.execute_query(
                   backend,
                   "select body from #{table}",
                   []
                 )

        assert total >= 1

        assert Enum.any?(rows, fn %{"body" => [body | _]} ->
                 body["event_message"] == message and
                   body["metadata"] == [%{"type" => "vector_log"}]
               end)
      end)
    end
  end

  describe "push_events/2 authentication" do
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
