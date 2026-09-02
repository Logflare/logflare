defmodule Logflare.BanditTelemetryLoggerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Logflare.BanditTelemetryLogger

  test "logs malformed requests with their route and connection" do
    {port, _server_id} = start_bandit()

    request =
      "POST /api/endpoints/query/path-secret?token=query-secret HTTP/1.1\r\n" <>
        "Host: localhost\r\n" <>
        "Transfer-Encoding: header-secret\r\n" <>
        "Connection: close\r\n\r\n" <>
        "body-secret"

    log =
      capture_log([level: :warning, format: "$message\n"], fn ->
        assert raw_request(port, request) =~ "HTTP/1.1 400 Bad Request"
      end)

    assert log =~
             ~r|Bandit request method=POST route=/api/endpoints/query/:token_or_name connection_id=#Reference<[^>]+> peer_ip=127\.0\.0\.1 stopped: request_error|

    refute log =~ "path-secret"
    refute log =~ "query-secret"
    refute log =~ "header-secret"
    refute log =~ "body-secret"
  end

  test "logs routed request exceptions without exposing their messages" do
    {port, _server_id} = start_bandit()

    request =
      "GET /sources/public/path-secret?token=query-secret HTTP/1.1\r\n" <>
        "Host: localhost\r\nConnection: close\r\n\r\n"

    log =
      capture_log([level: :error, format: "$message\n"], fn ->
        assert raw_request(port, request) =~ "HTTP/1.1 500 Internal Server Error"
      end)

    assert log =~
             ~r|Bandit request method=GET route=/sources/public/:public_token connection_id=#Reference<[^>]+> peer_ip=127\.0\.0\.1 crashed: kind=exit type=RuntimeError http_status=500|

    refute log =~ "path-secret"
    refute log =~ "query-secret"
    refute log =~ "exception-secret"
  end

  test "does not expose arbitrary exception terms" do
    {port, _server_id} = start_bandit()

    request =
      "GET /sources/public/malformed-path-secret?token=query-secret HTTP/1.1\r\n" <>
        "Host: localhost\r\nConnection: close\r\n\r\n"

    log =
      capture_log([level: :error, format: "$message\n"], fn ->
        assert raw_request(port, request) =~ "HTTP/1.1 500 Internal Server Error"
      end)

    assert log =~
             ~r|Bandit request method=GET route=/sources/public/:public_token connection_id=#Reference<[^>]+> peer_ip=127\.0\.0\.1 crashed: kind=throw type=unknown_error http_status=500|

    refute log =~ "malformed-path-secret"
    refute log =~ "query-secret"
    refute log =~ "exception-struct-secret"
  end

  test "logs failed TLS connections with their peer and measurements" do
    {port, server_id} =
      start_bandit(
        scheme: :https,
        certfile: Application.app_dir(:logflare, "priv/keys/localhost.cert"),
        keyfile: Application.app_dir(:logflare, "priv/keys/localhost.key")
      )

    log =
      capture_log([level: :warning, format: "$message\n"], fn ->
        _response = raw_request(port, "GET /tls-path-secret HTTP/1.1\r\nHost: localhost\r\n\r\n")
        :ok = stop_supervised(server_id)
      end)

    assert log =~
             ~r|Bandit connection connection_id=#Reference<[^>]+> peer_ip=127\.0\.0\.1 peer_port=\d+ duration_ms=\d+ received_bytes=unknown sent_bytes=unknown stopped: tls_alert|

    refute log =~ "tls-path-secret"
  end

  test "ignores successful requests and client errors" do
    {port, _server_id} = start_bandit()

    log =
      capture_log([level: :warning, format: "$message\n"], fn ->
        assert raw_request(
                 port,
                 "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
               ) =~ "HTTP/1.1 200 OK"

        assert raw_request(
                 port,
                 "GET /bad-request HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
               ) =~ "HTTP/1.1 400 Bad Request"
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
        thousand_island_options: [num_acceptors: 1, read_timeout: to_timeout(second: 1)]
      ]
      |> Keyword.merge(extra_options)

    child_spec = Bandit.child_spec(options)
    server = start_supervised!(child_spec)
    {:ok, {_address, port}} = ThousandIsland.listener_info(server)

    {port, child_spec.id}
  end

  defp serve_request(
         %Plug.Conn{request_path: "/sources/public/malformed-path-secret"},
         _opts
       ) do
    throw(%{__exception__: true, __struct__: "exception-struct-secret"})
  end

  defp serve_request(%Plug.Conn{request_path: "/sources/public/" <> _token}, _opts) do
    raise RuntimeError, "exception-secret"
  end

  defp serve_request(%Plug.Conn{request_path: "/bad-request"}, _opts) do
    raise Plug.BadRequestError
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
        [mode: :binary, packet: :raw, active: false],
        to_timeout(second: 1)
      )

    try do
      :ok = :gen_tcp.send(socket, request)
      receive_response(socket, [])
    after
      :gen_tcp.close(socket)
    end
  end

  defp receive_response(socket, chunks) do
    case :gen_tcp.recv(socket, 0, to_timeout(second: 2)) do
      {:ok, chunk} ->
        receive_response(socket, [chunk | chunks])

      {:error, reason} when reason in [:closed, :econnreset] ->
        chunks |> Enum.reverse() |> IO.iodata_to_binary()

      {:error, reason} ->
        flunk("failed to receive Bandit response: #{inspect(reason)}")
    end
  end
end
