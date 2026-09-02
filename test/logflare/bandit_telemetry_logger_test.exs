defmodule Logflare.BanditTelemetryLoggerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Logflare.BanditTelemetryLogger

  describe "Bandit requests" do
    setup do
      %{port: start_bandit()}
    end

    test "logs malformed requests with their route and connection", %{port: port} do
      forwarder = attach_forwarder([:bandit, :request, :stop])

      request =
        "POST /api/endpoints/query/path-secret?token=query-secret HTTP/1.1\r\n" <>
          "Host: localhost\r\n" <>
          "Transfer-Encoding: header-secret\r\n" <>
          "Connection: close\r\n\r\n" <>
          "body-secret"

      {metadata, log} =
        with_log([level: :warning, format: "$message\n"], fn ->
          assert raw_request(port, request) =~ "HTTP/1.1 400 Bad Request"

          {_measurements, metadata, connection_pid} =
            receive_event(forwarder, [:bandit, :request, :stop])

          await_termination(connection_pid)
          Logger.flush()
          metadata
        end)

      assert %Plug.Conn{} = metadata.conn

      assert log =~
               "Bandit request method=POST route=/api/endpoints/query/:token_or_name " <>
                 "connection_id=#{inspect(metadata.connection_telemetry_span_context)} " <>
                 "peer_ip=127.0.0.1 stopped: request_error"

      refute log =~ "path-secret"
      refute log =~ "query-secret"
      refute log =~ "header-secret"
      refute log =~ "body-secret"
    end

    test "logs routed request exceptions without exposing their messages", %{port: port} do
      forwarder = attach_forwarder([:bandit, :request, :exception])

      request =
        "GET /sources/public/path-secret?token=query-secret HTTP/1.1\r\n" <>
          "Host: localhost\r\nConnection: close\r\n\r\n"

      {metadata, log} =
        with_log([level: :error, format: "$message\n"], fn ->
          assert raw_request(port, request) =~ "HTTP/1.1 500 Internal Server Error"

          {_measurements, metadata, connection_pid} =
            receive_event(forwarder, [:bandit, :request, :exception])

          await_termination(connection_pid)
          Logger.flush()
          metadata
        end)

      assert log =~
               "Bandit request method=GET route=/sources/public/:public_token " <>
                 "connection_id=#{inspect(metadata.connection_telemetry_span_context)} " <>
                 "peer_ip=127.0.0.1 crashed: kind=exit type=RuntimeError http_status=500"

      refute log =~ "path-secret"
      refute log =~ "query-secret"
      refute log =~ "exception-secret"
    end
  end

  test "logs failed TLS connections with their peer and measurements" do
    port =
      start_bandit(
        scheme: :https,
        certfile: Application.app_dir(:logflare, "priv/keys/localhost.cert"),
        keyfile: Application.app_dir(:logflare, "priv/keys/localhost.key")
      )

    forwarder = attach_forwarder([:thousand_island, :connection, :stop])

    {event, log} =
      with_log([level: :warning, format: "$message\n"], fn ->
        _response = raw_request(port, "GET /tls-path-secret HTTP/1.1\r\nHost: localhost\r\n\r\n")

        {measurements, metadata, connection_pid} =
          receive_event(forwarder, [:thousand_island, :connection, :stop])

        await_termination(connection_pid)
        Logger.flush()
        {measurements, metadata}
      end)

    {measurements, metadata} = event
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    assert metadata.error |> elem(0) == :tls_alert

    assert log =~
             "Bandit connection connection_id=#{inspect(metadata.telemetry_span_context)} " <>
               "peer_ip=127.0.0.1 peer_port=#{metadata.remote_port} " <>
               "duration_ms=#{duration_ms} received_bytes=unknown sent_bytes=unknown " <>
               "stopped: tls_alert"

    refute log =~ "tls-path-secret"
  end

  test "does not expose malformed exception metadata" do
    connection_id = make_ref()

    log =
      capture_log(fn ->
        :telemetry.execute(
          [:bandit, :request, :exception],
          %{},
          %{
            connection_telemetry_span_context: connection_id,
            exception: %{__exception__: true, __struct__: "status-secret"},
            kind: :throw
          }
        )

        :telemetry.execute(
          [:thousand_island, :connection, :exception],
          %{},
          %{
            telemetry_span_context: connection_id,
            handler: Bandit.DelegatingHandler,
            reason: %{__struct__: "reason-secret"},
            kind: :throw
          }
        )
      end)

    assert log =~ "Bandit request method=unknown route=unknown"
    assert log =~ "type=unknown_error http_status=500"
    assert log =~ "Bandit connection connection_id=#{inspect(connection_id)}"
    assert log =~ "kind=throw reason=unknown_error"
    refute log =~ "status-secret"
    refute log =~ "reason-secret"
  end

  test "ignores routine request and connection events" do
    log =
      capture_log(fn ->
        :telemetry.execute([:bandit, :request, :stop], %{}, %{conn: Plug.Test.conn(:get, "/")})

        :telemetry.execute(
          [:bandit, :request, :stop],
          %{},
          %{error: "Unrecoverable error: closed"}
        )

        :telemetry.execute(
          [:bandit, :request, :exception],
          %{},
          %{exception: %Plug.BadRequestError{}, kind: :exit}
        )

        connection = %{
          handler: Bandit.DelegatingHandler,
          remote_address: {192, 0, 2, 1},
          remote_port: 1234
        }

        :telemetry.execute([:thousand_island, :connection, :stop], %{}, connection)

        :telemetry.execute(
          [:thousand_island, :connection, :stop],
          %{},
          Map.put(connection, :error, :timeout)
        )

        :telemetry.execute(
          [:thousand_island, :connection, :stop],
          %{},
          Map.put(connection, :error, :normal)
        )
      end)

    assert log == ""
  end

  test "detaches the telemetry handler" do
    on_exit(&BanditTelemetryLogger.attach/0)

    assert Enum.any?(
             :telemetry.list_handlers([:bandit, :request, :stop]),
             &(&1.id == BanditTelemetryLogger)
           )

    assert :ok = BanditTelemetryLogger.detach()

    refute Enum.any?(
             :telemetry.list_handlers([:bandit, :request, :stop]),
             &(&1.id == BanditTelemetryLogger)
           )
  end

  defp start_bandit(extra_options \\ []) do
    options =
      [
        plug: &serve_request/2,
        port: 0,
        ip: {127, 0, 0, 1},
        startup_log: false,
        http_options: [
          log_protocol_errors: false,
          log_client_closures: false,
          log_exceptions_with_status_codes: []
        ],
        http_2_options: [enabled: false],
        thousand_island_options: [num_acceptors: 1, read_timeout: 1_000]
      ]
      |> Keyword.merge(extra_options)

    server = start_supervised!({Bandit, options})
    {:ok, {_address, port}} = ThousandIsland.listener_info(server)
    port
  end

  defp serve_request(%Plug.Conn{request_path: "/sources/public/" <> _token}, _opts) do
    raise RuntimeError, "exception-secret"
  end

  defp serve_request(conn, _opts) do
    {:ok, _body, conn} = Plug.Conn.read_body(conn)
    Plug.Conn.send_resp(conn, 200, "ok")
  end

  defp raw_request(port, request) do
    {:ok, socket} =
      :gen_tcp.connect(
        {127, 0, 0, 1},
        port,
        [active: false, linger: {true, 0}, mode: :binary, nodelay: true],
        1_000
      )

    try do
      :ok = :gen_tcp.send(socket, request)
      receive_response(socket, [])
    after
      :gen_tcp.close(socket)
    end
  end

  defp receive_response(socket, chunks) do
    case :gen_tcp.recv(socket, 0, 2_000) do
      {:ok, chunk} ->
        receive_response(socket, [chunk | chunks])

      {:error, reason} when reason in [:closed, :econnreset] ->
        chunks |> Enum.reverse() |> IO.iodata_to_binary()

      {:error, reason} ->
        flunk("failed to receive Bandit response: #{inspect(reason)}")
    end
  end

  defp attach_forwarder(event) do
    handler_id = {__MODULE__, make_ref()}
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        event,
        fn event, measurements, metadata, {test_pid, handler_id} ->
          send(test_pid, {handler_id, event, measurements, metadata, self()})
        end,
        {test_pid, handler_id}
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    handler_id
  end

  defp receive_event(handler_id, event) do
    assert_receive {^handler_id, ^event, measurements, metadata, emitter_pid}, 2_000
    {measurements, metadata, emitter_pid}
  end

  defp await_termination(pid) do
    monitor = Process.monitor(pid)
    assert_receive {:DOWN, ^monitor, :process, ^pid, _reason}, 2_000
  end
end
