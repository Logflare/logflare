defmodule Logflare.DelegatingHandlerLoggerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Logflare.DelegatingHandlerLogger

  @logger_handler :delegating_handler_logger_test

  setup do
    :logger.remove_handler(@logger_handler)

    :ok =
      :logger.add_handler(@logger_handler, __MODULE__, %{
        level: :all,
        config: %{pid: self()}
      })

    on_exit(fn -> :logger.remove_handler(@logger_handler) end)

    :ok
  end

  test "logs abnormal TLS connection stop events with their peer and measurements" do
    {port, server_id} =
      start_bandit(
        scheme: :https,
        certfile: Application.app_dir(:logflare, "priv/keys/localhost.cert"),
        keyfile: Application.app_dir(:logflare, "priv/keys/localhost.key")
      )

    log =
      capture_log([level: :error], fn ->
        _response = raw_request(port, "GET /tls-path-secret HTTP/1.1\r\nHost: localhost\r\n\r\n")
        :ok = stop_supervised(server_id)
      end)

    assert length(Regex.scan(~r/Bandit DelegatingHandler connection failure/, log)) == 2

    for _event_number <- 1..2 do
      assert_receive {:log_event,
                      %{
                        level: :error,
                        meta: %{
                          bandit_connection: %{
                            duration_ms: duration_ms,
                            error: "tls_alert",
                            event: "stop",
                            peer_ip: "127.0.0.1",
                            peer_port: peer_port
                          }
                        }
                      } = event}

      assert is_integer(duration_ms)
      assert duration_ms >= 0
      assert is_integer(peer_port)
      refute inspect(event) =~ "tls-path-secret"
    end
  end

  test "logs available socket measurements as structured metadata" do
    duration = System.convert_time_unit(125, :millisecond, :native)

    capture_log([level: :error], fn ->
      :telemetry.execute(
        [:thousand_island, :connection, :stop],
        %{duration: duration, recv_oct: 2_048, send_oct: 1_024},
        %{
          error: {%Bandit.TransportError{message: "transport-secret", error: :econnreset}, []},
          handler: Bandit.DelegatingHandler,
          remote_address: {192, 0, 2, 8},
          remote_port: 12_345
        }
      )
    end)

    assert_receive {:log_event,
                    %{
                      level: :error,
                      meta: %{bandit_connection: connection}
                    } = event}

    assert connection == %{
             duration_ms: 125,
             error: "econnreset",
             event: "stop",
             peer_ip: "192.0.2.8",
             peer_port: 12_345,
             received_bytes: 2_048,
             sent_bytes: 1_024
           }

    refute inspect(event) =~ "transport-secret"
  end

  test "ignores normal connection closes" do
    {port, server_id} = start_bandit()

    capture_log([level: :error], fn ->
      assert raw_request(
               port,
               "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
             ) =~ "HTTP/1.1 200 OK"

      :ok = stop_supervised(server_id)
    end)

    refute_receive {:log_event, %{meta: %{bandit_connection: _connection}}}, 100
  end

  test "logs connection exception events introduced after Thousand Island 1.4" do
    duration = System.convert_time_unit(125, :millisecond, :native)

    log =
      capture_log([level: :error], fn ->
        :telemetry.execute(
          [:thousand_island, :connection, :exception],
          %{monotonic_time: System.monotonic_time(), duration: duration},
          %{
            handler: Bandit.DelegatingHandler,
            telemetry_span_context: make_ref(),
            remote_address: {192, 0, 2, 8},
            remote_port: 12_345,
            kind: :error,
            reason: %RuntimeError{message: "exception-secret"},
            stacktrace: []
          }
        )
      end)

    assert_receive {:log_event,
                    %{
                      level: :error,
                      meta: %{
                        bandit_connection:
                          %{
                            duration_ms: 125,
                            error: "RuntimeError",
                            event: "exception",
                            kind: "error",
                            peer_ip: "192.0.2.8",
                            peer_port: 12_345
                          } = connection
                      }
                    } = event}

    assert log =~ "Bandit DelegatingHandler connection failure"
    refute Map.has_key?(connection, :received_bytes)
    refute Map.has_key?(connection, :sent_bytes)
    refute inspect(event) =~ "exception-secret"
  end

  test "detaches the telemetry handler" do
    on_exit(&DelegatingHandlerLogger.attach/0)

    assert Enum.any?(
             :telemetry.list_handlers([:thousand_island, :connection, :stop]),
             &(&1.id == DelegatingHandlerLogger)
           )

    assert :ok = DelegatingHandlerLogger.detach()

    refute Enum.any?(
             :telemetry.list_handlers([:thousand_island, :connection, :stop]),
             &(&1.id == DelegatingHandlerLogger)
           )
  end

  def log(event, %{config: %{pid: pid}}) do
    send(pid, {:log_event, event})
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
