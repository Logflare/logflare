defmodule Logflare.BanditTelemetryLoggerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Logflare.BanditTelemetryLogger

  test "logs abnormal connections with their peer and measurements" do
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

  test "ignores normal connection closes" do
    {port, server_id} = start_bandit()

    log =
      capture_log([level: :warning, format: "$message\n"], fn ->
        assert raw_request(
                 port,
                 "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
               ) =~ "HTTP/1.1 200 OK"

        :ok = stop_supervised(server_id)
      end)

    assert log == ""
  end

  test "detaches the telemetry handler" do
    on_exit(&BanditTelemetryLogger.attach/0)

    assert Enum.any?(
             :telemetry.list_handlers([:thousand_island, :connection, :stop]),
             &(&1.id == BanditTelemetryLogger)
           )

    assert :ok = BanditTelemetryLogger.detach()

    refute Enum.any?(
             :telemetry.list_handlers([:thousand_island, :connection, :stop]),
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
        thousand_island_options: [num_acceptors: 1]
      ]
      |> Keyword.merge(extra_options)

    child_spec = Bandit.child_spec(options)
    server = start_supervised!(child_spec)
    {:ok, {_address, port}} = ThousandIsland.listener_info(server)

    {port, child_spec.id}
  end

  defp serve_request(conn, _opts) do
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
