defmodule Logflare.IngressDrainTest do
  use ExUnit.Case, async: false

  alias Logflare.IngressDrain
  alias Logflare.IngressGate
  alias Logflare.Test.ShutdownProbeEndpoint
  alias Logflare.Test.ShutdownProbeGrpc
  alias Logflare.Test.ShutdownProbePlug
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest
  alias LogflareWeb.Plugs.Shutdown

  setup do
    :ets.delete_all_objects(IngressGate)
    :ets.insert(IngressGate, {:admission, :open})

    on_exit(fn ->
      :ets.delete_all_objects(IngressGate)
      :ets.insert(IngressGate, {:admission, :open})
    end)
  end

  test "HTTP/2: finishes the admitted stream, refuses new TCP and new work on the existing connection" do
    {server, port} = http_server()
    conn = h2(port)
    {:ok, conn, held_ref} = Mint.HTTP.request(conn, "GET", "/hold", [], nil)
    assert_receive {:held_http, handler}, 2_000
    assert IngressGate.active_count() == 1

    task =
      Task.async(fn ->
        IngressDrain.run(deadline(), http_listeners: [server], grpc_listener: nil)
      end)

    await_suspended(server)
    assert {:error, :econnrefused} = tcp_connect(port)
    {:ok, conn, rejected_ref} = Mint.HTTP.request(conn, "GET", "/new", [], nil)
    {conn, rejected} = until_done(conn, rejected_ref)
    assert {:status, rejected_ref, 503} in rejected
    assert Process.alive?(handler)
    assert IngressGate.active_count() == 1
    refute Task.yield(task, 0)

    send(handler, :release)
    {conn, accepted} = until_done(conn, held_ref, rejected)
    assert {:status, held_ref, 200} in accepted
    assert Task.await(task) == :drained
    assert Process.alive?(server)
    assert ThousandIsland.status(server) == :suspended
    Mint.HTTP.close(conn)
  end

  test "HTTP/1: an idle keep-alive connection cannot bypass the closed gate" do
    {server, port} = http_server()
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1], mode: :passive)
    {:ok, conn, first} = Mint.HTTP.request(conn, "GET", "/", [], nil)
    {conn, responses} = until_done(conn, first)
    assert {:status, first, 200} in responses
    assert IngressDrain.run(deadline(), http_listeners: [server], grpc_listener: nil) == :drained
    {:ok, conn, second} = Mint.HTTP.request(conn, "GET", "/", [], nil)
    {conn, responses} = until_done(conn, second)
    assert {:status, second, 503} in responses
    Mint.HTTP.close(conn)
  end

  test "gRPC: admitted callback completes, but a new RPC on the same connection is unavailable" do
    :persistent_term.put({ShutdownProbeGrpc, :test_pid}, self())
    on_exit(fn -> :persistent_term.erase({ShutdownProbeGrpc, :test_pid}) end)

    supervisor =
      start_supervised!(
        {GRPC.Server.Supervisor,
         endpoint: ShutdownProbeEndpoint,
         port: 0,
         start_server: true,
         adapter_opts: [
           ip: {127, 0, 0, 1},
           goaway_initial_timeout: 0,
           goaway_complete_timeout: 5_000
         ]}
      )

    ref = inspect(ShutdownProbeEndpoint)
    port = :ranch.get_port(ref)
    conn = h2(port)
    path = "/opentelemetry.proto.collector.logs.v1.LogsService/Export"
    encoded = ExportLogsServiceRequest.encode(%ExportLogsServiceRequest{})
    body = <<0, byte_size(encoded)::32, encoded::binary>>
    headers = [{"content-type", "application/grpc"}, {"te", "trailers"}]
    {:ok, conn, held_ref} = Mint.HTTP.request(conn, "POST", path, headers, body)
    assert_receive {:held_rpc, handler}, 2_000

    task =
      Task.async(fn -> IngressDrain.run(deadline(), http_listeners: [], grpc_listener: ref) end)

    await_grpc_suspended(ref)
    assert {:error, :econnrefused} = tcp_connect(port)
    {:ok, conn, rejected_ref} = Mint.HTTP.request(conn, "POST", path, headers, body)
    {conn, rejected} = until_done(conn, rejected_ref)
    assert grpc_status(rejected, rejected_ref) == "14"
    assert Process.alive?(handler)
    refute Task.yield(task, 0)

    send(handler, :release)
    {conn, accepted} = until_done(conn, held_ref, rejected)
    assert grpc_status(accepted, held_ref) == "0"
    assert Task.await(task) == :drained
    assert Process.alive?(supervisor)
    assert :ranch.get_status(ref) == :suspended
    assert :ranch.get_transport_options(ref).shutdown > 0
    Mint.HTTP.close(conn)
  end

  test "endpoint wrapper rejects normal routes and socket upgrades but preserves health endpoints" do
    IngressGate.close()

    for path <- ["/", "/logs/websocket", "/live/websocket"] do
      conn = Plug.Test.conn(:get, path) |> LogflareWeb.Endpoint.call([])
      assert conn.status == 503
      assert conn.halted
    end

    conn = Plug.Test.conn(:get, "/health")
    refute Shutdown.call(conn, []).halted
  end

  test "already-connected websocket clients cannot ingest another batch after gate closure" do
    IngressGate.close()

    socket = %Phoenix.Socket{
      transport_pid: self(),
      serializer: Phoenix.ChannelTest.NoopSerializer,
      topic: "logs:shutdown",
      joined: true,
      join_ref: "1"
    }

    assert {:noreply, _socket} =
             LogflareWeb.LogChannel.handle_in("batch", %{"batch" => []}, socket)

    assert_receive %Phoenix.Socket.Message{event: "batch", payload: %{message: "Batch error"}}
  end

  defp http_server do
    server =
      start_supervised!(
        {Bandit,
         plug: {ShutdownProbePlug, self()}, port: 0, ip: {127, 0, 0, 1}, startup_log: false}
      )

    {:ok, {_address, port}} = ThousandIsland.listener_info(server)
    {server, port}
  end

  defp h2(port) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http2], mode: :passive)
    conn
  end

  defp until_done(conn, ref, responses \\ [], attempts \\ 20) do
    if {:done, ref} in responses do
      {conn, responses}
    else
      assert attempts > 0, "response did not complete: #{inspect(responses)}"
      {:ok, conn, next} = Mint.HTTP.recv(conn, 0, 2_000)
      until_done(conn, ref, responses ++ next, attempts - 1)
    end
  end

  defp tcp_connect(port),
    do: :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)

  defp deadline, do: System.monotonic_time(:millisecond) + 5_000

  defp await_suspended(server),
    do: await(fn -> ThousandIsland.status(server) == :suspended end, deadline())

  defp await_grpc_suspended(ref),
    do: await(fn -> :ranch.get_status(ref) == :suspended end, deadline())

  defp await(condition, deadline) do
    unless condition.() do
      assert System.monotonic_time(:millisecond) < deadline

      receive do
      after
        10 -> :ok
      end

      await(condition, deadline)
    end
  end

  defp grpc_status(responses, ref) do
    Enum.find_value(responses, fn
      {:headers, ^ref, headers} -> List.keyfind(headers, "grpc-status", 0)
      _ -> nil
    end)
    |> elem(1)
  end
end
