defmodule Logflare.BanditLoggerMetadataTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  require Logger

  alias Logflare.BanditLoggerMetadata

  setup do
    :ok =
      :logger.add_handler(__MODULE__, __MODULE__, %{
        level: :all,
        config: %{pid: self()}
      })

    on_exit(fn -> :logger.remove_handler(__MODULE__) end)
    :ok
  end

  test "registers connection and request telemetry handlers at startup" do
    for event <- [
          [:thousand_island, :connection, :start],
          [:thousand_island, :connection, :stop],
          [:thousand_island, :connection, :exception],
          [:bandit, :request, :start]
        ] do
      assert Enum.any?(:telemetry.list_handlers(event), &(&1.id == BanditLoggerMetadata))
    end
  end

  test "keeps the connection ID across HTTP/1 requests after metadata is cleared" do
    client = connect(start_bandit())

    capture_log(fn ->
      request(client, "/first")
      assert_receive {:request, pid, "/first", metadata}
      connection_id = Keyword.fetch!(metadata, :connection_id)
      assert is_binary(connection_id)
      assert byte_size(connection_id) > 0

      assert_receive {:log_event, %{meta: %{connection_id: ^connection_id, path: "/first"}}}

      :sys.get_state(pid)
      {:dictionary, dictionary} = Process.info(pid, :dictionary)
      refute Keyword.has_key?(dictionary, :"$logger_metadata$")

      request(client, "/second")
      assert_receive {:request, ^pid, "/second", second_metadata}
      assert Keyword.fetch!(second_metadata, :connection_id) == connection_id

      assert_receive {:log_event, %{meta: %{connection_id: ^connection_id, path: "/second"}}}
    end)
  end

  test "assigns different IDs to different connections" do
    port = start_bandit()
    first_client = connect(port)
    second_client = connect(port)

    capture_log(fn ->
      request(first_client, "/first")
      assert_receive {:request, first_pid, "/first", first_metadata}
      request(second_client, "/second")
      assert_receive {:request, second_pid, "/second", second_metadata}

      refute first_pid == second_pid

      refute Keyword.fetch!(first_metadata, :connection_id) ==
               Keyword.fetch!(second_metadata, :connection_id)
    end)
  end

  test "concurrent HTTP/2 streams share the connection ID in separate processes" do
    {:ok, client} = Mint.HTTP2.connect(:http, "127.0.0.1", start_bandit(), mode: :passive)
    on_exit(fn -> Mint.HTTP2.close(client) end)

    capture_log(fn ->
      {:ok, client, first_ref} =
        Mint.HTTP2.request(client, "GET", "/waiting/first", [], nil)

      {:ok, client, second_ref} =
        Mint.HTTP2.request(client, "GET", "/waiting/second", [], nil)

      assert_receive {:request, first_pid, "/waiting/first", first_metadata}
      assert_receive {:request, second_pid, "/waiting/second", second_metadata}
      refute first_pid == second_pid
      connection_id = Keyword.fetch!(first_metadata, :connection_id)
      assert Keyword.fetch!(second_metadata, :connection_id) == connection_id

      assert_receive {:log_event,
                      %{meta: %{connection_id: ^connection_id, path: "/waiting/first"}}}

      assert_receive {:log_event,
                      %{meta: %{connection_id: ^connection_id, path: "/waiting/second"}}}

      send(first_pid, :respond)
      send(second_pid, :respond)
      receive_http2_responses(client, MapSet.new([first_ref, second_ref]))
    end)
  end

  test "restores the ID on the existing idle connection crash without another error" do
    client = connect(start_bandit())

    capture_log([level: :error], fn ->
      request(client, "/before-idle")
      assert_receive {:request, pid, "/before-idle", metadata}
      connection_id = Keyword.fetch!(metadata, :connection_id)
      {%ThousandIsland.Socket{socket: socket}, _state} = :sys.get_state(pid)
      {:dictionary, dictionary} = Process.info(pid, :dictionary)
      refute Keyword.has_key?(dictionary, :"$logger_metadata$")

      assert {:thousand_island, :connection,
              {Bandit.DelegatingHandler, {{127, 0, 0, 1}, peer_port}}} =
               :proc_lib.get_label(pid)

      monitor = Process.monitor(pid)
      send(pid, {:tcp_error, socket, :econnreset})
      assert_receive {:DOWN, ^monitor, :process, ^pid, :econnreset}

      assert_receive {:log_event,
                      %{level: :error, meta: %{pid: ^pid, connection_id: ^connection_id}} =
                        event}

      message = event |> Logger.Formatter.format_event(8192) |> IO.chardata_to_string()
      assert message =~ "** (stop) :econnreset"
      assert message =~ "Process Label:"
      assert message =~ "127, 0, 0, 1"
      assert message =~ Integer.to_string(peer_port)
      assert message =~ "Last message: {:tcp_error"

      refute_receive {:log_event, %{level: :error, meta: %{connection_id: ^connection_id}}}
    end)
  end

  test "restores connection metadata on stop and exception and preserves request metadata" do
    ref = make_ref()
    Logger.metadata(request_id: "request-123")

    :ok =
      BanditLoggerMetadata.handle_event(
        [:bandit, :request, :start],
        %{},
        %{connection_telemetry_span_context: ref},
        []
      )

    connection_id = Logger.metadata()[:connection_id]
    assert is_binary(connection_id)
    assert Logger.metadata()[:request_id] == "request-123"

    for event <- [:stop, :exception] do
      Logger.metadata(connection_id: nil)

      :ok =
        BanditLoggerMetadata.handle_event(
          [:thousand_island, :connection, event],
          %{},
          %{handler: Bandit.DelegatingHandler, telemetry_span_context: ref},
          []
        )

      assert Logger.metadata()[:connection_id] == connection_id
      assert Logger.metadata()[:request_id] == "request-123"
    end

    refute_receive {:log_event, _event}
  end

  test "ignores unrelated and malformed telemetry without changing metadata" do
    Logger.metadata(connection_id: "existing-connection", request_id: "existing-request")
    original_metadata = Logger.metadata()

    for {event, metadata} <- [
          {[:unrelated], %{connection_telemetry_span_context: make_ref()}},
          {[:thousand_island, :connection, :start],
           %{handler: __MODULE__, telemetry_span_context: make_ref()}},
          {[:thousand_island, :connection, :stop],
           %{handler: Bandit.DelegatingHandler, telemetry_span_context: nil}},
          {[:bandit, :request, :start], %{connection_telemetry_span_context: "invalid"}},
          {[:bandit, :request, :start], %{}}
        ] do
      assert :ok = BanditLoggerMetadata.handle_event(event, %{}, metadata, [])
      assert Logger.metadata() == original_metadata
    end

    refute_receive {:log_event, _event}
  end

  def log(event, %{config: %{pid: pid}}), do: send(pid, {:log_event, event})

  defp start_bandit do
    caller = self()

    server =
      start_supervised!(
        {Bandit,
         plug: fn conn, _opts -> serve_request(conn, caller) end,
         ip: {127, 0, 0, 1},
         port: 0,
         startup_log: false,
         thousand_island_options: [num_acceptors: 1]}
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    port
  end

  defp serve_request(conn, caller) do
    Logger.info("connection metadata request", method: conn.method, path: conn.request_path)
    send(caller, {:request, self(), conn.request_path, Logger.metadata()})
    send_response(conn)
  end

  defp send_response(%{request_path: "/waiting/" <> _path} = conn) do
    receive do
      :respond -> Plug.Conn.send_resp(conn, 200, "ok")
    after
      to_timeout(second: 1) -> raise "request was not released"
    end
  end

  defp send_response(conn), do: Plug.Conn.send_resp(conn, 200, "ok")

  defp connect(port) do
    {:ok, client} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], to_timeout(second: 1))

    on_exit(fn -> :gen_tcp.close(client) end)
    client
  end

  defp request(client, path) do
    :ok = :gen_tcp.send(client, "GET #{path} HTTP/1.1\r\nHost: localhost\r\n\r\n")
    receive_response(client, "")
  end

  defp receive_response(client, response) do
    case String.split(response, "\r\n\r\n", parts: 2) do
      [headers, "ok"] ->
        assert headers =~ "200 OK"

      _response ->
        {:ok, data} = :gen_tcp.recv(client, 0, to_timeout(second: 1))
        receive_response(client, response <> data)
    end
  end

  defp receive_http2_responses(client, pending_refs) do
    case MapSet.size(pending_refs) do
      0 ->
        Mint.HTTP2.close(client)

      _count ->
        {:ok, client, responses} = Mint.HTTP2.recv(client, 0, to_timeout(second: 1))

        pending_refs =
          Enum.reduce(responses, pending_refs, fn
            {:done, ref}, refs -> MapSet.delete(refs, ref)
            _response, refs -> refs
          end)

        receive_http2_responses(client, pending_refs)
    end
  end
end
