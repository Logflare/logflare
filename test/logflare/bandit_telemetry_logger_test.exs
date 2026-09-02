defmodule Logflare.BanditTelemetryLoggerTest do
  use ExUnit.Case, async: false

  alias Logflare.BanditTelemetryLogger

  @moduletag capture_log: true
  @capture_handler :bandit_telemetry_logger_test_capture

  setup do
    Logger.reset_metadata(existing: "metadata")
    :ok = BanditTelemetryLogger.attach()

    :ok =
      :logger.add_handler(@capture_handler, Logflare.TestLoggerCaptureHandler, %{
        level: :all,
        config: %{pid: self()}
      })

    on_exit(fn -> :logger.remove_handler(@capture_handler) end)

    :ok
  end

  test "uses a shared connection id in failed request and connection logs" do
    connection_id = make_ref()
    duration = System.convert_time_unit(125, :millisecond, :native)
    Logger.metadata(request_id: "http-request-id")

    conn =
      :post
      |> Plug.Test.conn(
        "https://api.logflare.app/api/endpoints/query/path-secret?token=query-secret",
        "body-secret"
      )

    :telemetry.execute(
      [:bandit, :request, :stop],
      %{duration: duration},
      %{
        connection_telemetry_span_context: connection_id,
        conn: conn,
        error: "Unrecoverable error: econnreset"
      }
    )

    assert_receive {:log_event,
                    request_event = %{
                      level: :warning,
                      msg: {:string, "Bandit request stopped with error"},
                      meta: %{
                        telemetry_event: "bandit.request.stop",
                        connection_id: logged_connection_id,
                        request_id: "http-request-id",
                        request: request,
                        peer_ip: "127.0.0.1",
                        error_reason: "econnreset"
                      }
                    }}

    assert request == %{
             method: "POST",
             route: "/api/endpoints/query/:token_or_name"
           }

    assert logged_connection_id == inspect(connection_id)

    :telemetry.execute(
      [:thousand_island, :connection, :stop],
      %{
        duration: duration,
        send_oct: 1_024,
        send_cnt: 4,
        recv_oct: 2_048,
        recv_cnt: 8
      },
      %{
        telemetry_span_context: connection_id,
        handler: Bandit.DelegatingHandler,
        remote_address: {127, 0, 0, 1},
        remote_port: 54_321,
        error: :econnreset
      }
    )

    assert_receive {:log_event,
                    connection_event = %{
                      level: :warning,
                      msg: {:string, "Bandit connection stopped with error"},
                      meta: %{
                        telemetry_event: "thousand_island.connection.stop",
                        connection_id: logged_connection_id,
                        peer_ip: "127.0.0.1",
                        peer_port: 54_321,
                        duration_ms: 125,
                        sent_bytes: 1_024,
                        received_bytes: 2_048,
                        error_reason: "econnreset",
                        exception_type: nil
                      }
                    }}

    assert logged_connection_id == inspect(connection_id)

    logged = inspect({request_event, connection_event})
    refute logged =~ "path-secret"
    refute logged =~ "query-secret"
    refute logged =~ "body-secret"
  end

  test "logs request exceptions without exposing exception messages" do
    connection_id = make_ref()
    conn = Plug.Test.conn(:get, "/sources/public/path-secret?token=query-secret")

    :telemetry.execute(
      [:bandit, :request, :exception],
      %{},
      %{
        connection_telemetry_span_context: connection_id,
        conn: conn,
        kind: :exit,
        exception: RuntimeError.exception("body-secret")
      }
    )

    assert_receive {:log_event,
                    event = %{
                      level: :error,
                      msg: {:string, "Bandit request crashed"},
                      meta: %{
                        telemetry_event: "bandit.request.exception",
                        connection_id: logged_connection_id,
                        request: request,
                        exception_kind: "exit",
                        exception_type: "RuntimeError",
                        http_status: 500
                      }
                    }}

    assert request == %{
             method: "GET",
             route: "/sources/public/:public_token"
           }

    assert logged_connection_id == inspect(connection_id)
    refute inspect(event) =~ "path-secret"
    refute inspect(event) =~ "query-secret"
    refute inspect(event) =~ "body-secret"
  end

  test "logs request errors before a conn exists without exposing wire data" do
    connection_id = make_ref()

    :telemetry.execute(
      [:bandit, :request, :stop],
      %{},
      %{
        connection_telemetry_span_context: connection_id,
        error: ~s(Header read HTTP error: "authorization: Bearer header-secret")
      }
    )

    assert_receive {:log_event,
                    event = %{
                      level: :warning,
                      meta: %{
                        telemetry_event: "bandit.request.stop",
                        connection_id: logged_connection_id,
                        request: %{},
                        error_reason: "request_error"
                      }
                    }}

    assert logged_connection_id == inspect(connection_id)
    refute inspect(event) =~ "header-secret"
  end

  test "logs connection exceptions from newer Thousand Island versions" do
    connection_id = make_ref()

    :telemetry.execute(
      [:thousand_island, :connection, :exception],
      %{},
      %{
        telemetry_span_context: connection_id,
        handler: Bandit.DelegatingHandler,
        remote_address: {192, 0, 2, 8},
        remote_port: 12_345,
        kind: :error,
        reason: RuntimeError.exception("exception-secret")
      }
    )

    assert_receive {:log_event,
                    event = %{
                      level: :error,
                      msg: {:string, "Bandit connection crashed"},
                      meta: %{
                        telemetry_event: "thousand_island.connection.exception",
                        connection_id: logged_connection_id,
                        peer_ip: "192.0.2.8",
                        peer_port: 12_345,
                        error_reason: nil,
                        exception_kind: "error",
                        exception_type: "RuntimeError"
                      }
                    }}

    assert logged_connection_id == inspect(connection_id)
    refute inspect(event) =~ "exception-secret"
  end

  test "ignores successful requests, normal connection closes, idle timeouts, and other handlers" do
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

    connection_metadata = %{
      handler: Bandit.DelegatingHandler,
      remote_address: {192, 0, 2, 1},
      remote_port: 1234
    }

    :telemetry.execute([:thousand_island, :connection, :stop], %{}, connection_metadata)

    :telemetry.execute(
      [:thousand_island, :connection, :stop],
      %{},
      Map.put(connection_metadata, :error, :timeout)
    )

    :telemetry.execute(
      [:thousand_island, :connection, :stop],
      %{},
      Map.put(connection_metadata, :error, :normal)
    )

    :telemetry.execute(
      [:thousand_island, :connection, :stop],
      %{},
      %{connection_metadata | handler: SomeOtherHandler} |> Map.put(:error, :econnreset)
    )

    refute_receive {:log_event, %{meta: %{telemetry_event: _event}}}, 100
    assert Map.new(Logger.metadata()) == %{existing: "metadata"}
  end
end
