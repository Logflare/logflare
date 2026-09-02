defmodule Logflare.BanditTelemetryLoggerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  test "logs failed requests and connections with a shared connection id" do
    connection_id = make_ref()
    duration = System.convert_time_unit(125, :millisecond, :native)

    conn =
      Plug.Test.conn(
        :post,
        "https://api.logflare.app/api/endpoints/query/path-secret?token=query-secret",
        "body-secret"
      )

    log =
      capture_log(fn ->
        :telemetry.execute(
          [:bandit, :request, :stop],
          %{},
          %{
            connection_telemetry_span_context: connection_id,
            conn: conn,
            error: "Unrecoverable error: econnreset"
          }
        )

        :telemetry.execute(
          [:thousand_island, :connection, :stop],
          %{duration: duration, send_oct: 1_024, recv_oct: 2_048},
          %{
            telemetry_span_context: connection_id,
            handler: Bandit.DelegatingHandler,
            remote_address: {127, 0, 0, 1},
            remote_port: 54_321,
            error: :econnreset
          }
        )
      end)

    id = inspect(connection_id)

    assert log =~
             "Bandit request method=POST route=/api/endpoints/query/:token_or_name " <>
               "connection_id=#{id} peer_ip=127.0.0.1 stopped: econnreset"

    assert log =~
             "Bandit connection connection_id=#{id} peer_ip=127.0.0.1 peer_port=54321 " <>
               "duration_ms=125 received_bytes=2048 sent_bytes=1024 stopped: econnreset"

    refute log =~ "path-secret"
    refute log =~ "query-secret"
    refute log =~ "body-secret"
  end

  test "logs request exceptions without exposing exception messages" do
    connection_id = make_ref()
    conn = Plug.Test.conn(:get, "/sources/public/path-secret?token=query-secret")

    log =
      capture_log(fn ->
        :telemetry.execute(
          [:bandit, :request, :exception],
          %{},
          %{
            connection_telemetry_span_context: connection_id,
            conn: conn,
            kind: :exit,
            exception: RuntimeError.exception("exception-secret")
          }
        )
      end)

    assert log =~
             "Bandit request method=GET route=/sources/public/:public_token " <>
               "connection_id=#{inspect(connection_id)} peer_ip=127.0.0.1 crashed: " <>
               "kind=exit type=RuntimeError http_status=500"

    refute log =~ "path-secret"
    refute log =~ "query-secret"
    refute log =~ "exception-secret"
  end

  test "logs request errors before a conn exists without exposing wire data" do
    connection_id = make_ref()

    log =
      capture_log(fn ->
        :telemetry.execute(
          [:bandit, :request, :stop],
          %{},
          %{
            connection_telemetry_span_context: connection_id,
            error: ~s(Header read HTTP error: "authorization: Bearer header-secret")
          }
        )
      end)

    assert log =~
             "Bandit request method=unknown route=unknown " <>
               "connection_id=#{inspect(connection_id)} peer_ip=unknown stopped: request_error"

    refute log =~ "header-secret"
  end

  test "logs connection exceptions without exposing exception messages" do
    connection_id = make_ref()
    duration = System.convert_time_unit(125, :millisecond, :native)

    log =
      capture_log(fn ->
        :telemetry.execute(
          [:thousand_island, :connection, :exception],
          %{duration: duration},
          %{
            telemetry_span_context: connection_id,
            handler: Bandit.DelegatingHandler,
            remote_address: {192, 0, 2, 8},
            remote_port: 12_345,
            kind: :error,
            reason: RuntimeError.exception("exception-secret")
          }
        )
      end)

    assert log =~
             "Bandit connection connection_id=#{inspect(connection_id)} " <>
               "peer_ip=192.0.2.8 peer_port=12345 duration_ms=125 " <>
               "received_bytes=unknown sent_bytes=unknown crashed: kind=error reason=RuntimeError"

    refute log =~ "exception-secret"
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
end
