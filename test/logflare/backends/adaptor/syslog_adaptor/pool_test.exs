defmodule Logflare.Backends.Adaptor.SyslogAdaptor.PoolTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Backends.Adaptor.SyslogAdaptor
  alias Logflare.Backends.Adaptor.SyslogAdaptor.Pool
  alias Logflare.Backends.Adaptor.SyslogAdaptor.Socket

  @telemetry_events [
    [:logflare, :syslog_pool, :connect, :start],
    [:logflare, :syslog_pool, :connect, :stop],
    [:logflare, :syslog_pool, :connect, :exception],
    [:logflare, :syslog_pool, :reused_connection],
    [:logflare, :syslog_pool, :disconnect]
  ]
  @secret "syslog-secret-do-not-emit"

  test "closes a connection when its first checkout is cancelled" do
    # Active mode delivers the peer's tcp_closed notification to this test process.
    {:ok, listen} =
      :gen_tcp.listen(0, mode: :binary, packet: :raw, active: true, reuseaddr: true)

    {:ok, {_address, port}} = :inet.sockname(listen)

    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend,
        type: :syslog,
        sources: [source],
        config: %{host: "127.0.0.1", port: port},
        user: user
      )

    pool =
      start_supervised!(
        {Pool,
         backend_id: backend.id, name: __MODULE__, worker_idle_timeout: to_timeout(minute: 1)}
      )

    test_pid = self()

    # Syslog has no response phase to wait on, so pause the borrower after the pool
    # records and takes ownership of the socket but before the worker checks back in.
    stub(Socket, :send, fn _socket, _message ->
      send(test_pid, {:socket_send, self()})
      Process.sleep(:infinity)
    end)

    borrower = start_supervised!({Task, fn -> Pool.send(pool, "test") end})
    {:ok, peer} = :gen_tcp.accept(listen, to_timeout(second: 1))

    # Reaching Socket.send/2 proves connect, NimblePool.update/2, and ownership transfer completed.
    assert_receive {:socket_send, ^borrower}, to_timeout(second: 1)

    # NimblePool should terminate the connected worker and close its pool-owned socket.
    Process.exit(borrower, :kill)
    assert_receive {:tcp_closed, ^peer}, to_timeout(second: 1)
  end

  describe "connection telemetry" do
    setup do
      {:ok, listen} = :gen_tcp.listen(0, mode: :binary, active: false, reuseaddr: true)
      {:ok, {_address, port}} = :inet.sockname(listen)

      config = %{
        host: "127.0.0.1",
        port: port,
        tls: false,
        client_key: @secret,
        cipher_key: @secret,
        client_cert: @secret,
        ca_cert: @secret,
        structured_data: @secret,
        custom_config: @secret
      }

      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :syslog, sources: [source], config: config, user: user)

      pool =
        start_supervised!(
          {Pool,
           backend_id: backend.id, name: __MODULE__, worker_idle_timeout: to_timeout(minute: 1)}
        )

      telemetry_ref = :telemetry_test.attach_event_handlers(self(), @telemetry_events)
      assert :ok = SyslogAdaptor.attach_logger()
      previous_level = Logger.level()
      Logger.configure(level: :debug)

      on_exit(fn ->
        :telemetry.detach(telemetry_ref)
        SyslogAdaptor.detach_logger()
        Logger.configure(level: previous_level)
        :gen_tcp.close(listen)
      end)

      %{
        backend_id: backend.id,
        config: config,
        listen: listen,
        pool: pool,
        source_id: source.id,
        telemetry_ref: telemetry_ref
      }
    end

    test "send/2 identifies the backend and keeps connection logging attached", context do
      %{backend_id: backend_id, pool: pool, source_id: source_id, telemetry_ref: ref} = context

      log =
        capture_log(fn ->
          assert :ok = Pool.send(pool, "first")
          {:ok, peer} = :gen_tcp.accept(context.listen, to_timeout(second: 1))
          assert {:ok, "first"} = :gen_tcp.recv(peer, 5, to_timeout(second: 1))

          for event <- [:start, :stop] do
            assert_receive {[:logflare, :syslog_pool, :connect, ^event], ^ref, measurements,
                            metadata}

            assert_span_metadata(metadata, backend_id, context.config)
            assert is_integer(measurements.monotonic_time)
          end

          assert :ok = Pool.send(pool, "second")

          assert_receive {[:logflare, :syslog_pool, :reused_connection], ^ref, %{system_time: _},
                          %{backend_id: ^backend_id} = metadata}

          assert metadata == %{backend_id: backend_id}

          assert :ok =
                   Pool.send(pool, "third", %{
                     backend_id: -1,
                     source_id: source_id,
                     config: %{client_key: @secret},
                     client_key: @secret
                   })

          assert_receive {[:logflare, :syslog_pool, :reused_connection], ^ref, _, metadata}
          assert metadata == %{backend_id: backend_id, source_id: source_id}
          :sys.get_state(pool)
          assert :ok = stop_supervised(Pool)

          assert_receive {[:logflare, :syslog_pool, :disconnect], ^ref, %{system_time: _},
                          metadata}

          assert metadata == %{
                   backend_id: backend_id,
                   config: Map.take(context.config, [:host, :port, :tls]),
                   reason: :shutdown
                 }

          assert_logger_attached()
        end)

      assert log =~ "[Syslog] Backend #{backend_id} connected to 127.0.0.1:"
      assert log =~ "[debug] [Syslog] Backend #{backend_id} reused connection"
      assert log =~ "[Syslog] Backend #{backend_id} disconnected from 127.0.0.1:"
      refute log =~ @secret
    end

    test "failed connections retain safe metadata and use the pool's backend identity", context do
      %{backend_id: backend_id, pool: pool, source_id: source_id, telemetry_ref: ref} = context

      expect(Socket, :connect, 2, fn config, _timeout ->
        assert config.client_key == @secret
        assert config.cipher_key == @secret
        {:error, :econnrefused}
      end)

      log =
        capture_log(fn ->
          for metadata <- [
                %{},
                %{
                  backend_id: -1,
                  source_id: source_id,
                  config: %{client_key: @secret},
                  client_key: @secret
                }
              ] do
            assert {:error, :econnrefused} = Pool.send(pool, "test", metadata)
            source_metadata = Map.take(metadata, [:source_id])

            assert_receive {[:logflare, :syslog_pool, :connect, :start], ^ref, _, metadata}
            assert_span_metadata(metadata, backend_id, context.config, source_metadata)

            assert_receive {[:logflare, :syslog_pool, :connect, :stop], ^ref, %{duration: _},
                            metadata}

            assert_span_metadata(
              metadata,
              backend_id,
              context.config,
              Map.merge(source_metadata, %{kind: :error, reason: :econnrefused})
            )

            assert_logger_attached()
          end
        end)

      assert log =~ "[warning] [Syslog] Backend #{backend_id} failed to connect to 127.0.0.1:"
      refute log =~ @secret
      refute log =~ "Backend -1"
    end

    test "connection exceptions retain safe metadata without detaching the logger", context do
      %{backend_id: backend_id, pool: pool, telemetry_ref: ref} = context
      expect(Socket, :connect, fn _config, _timeout -> raise "connection failed" end)

      log =
        capture_log(fn ->
          assert_raise RuntimeError, "connection failed", fn -> Pool.send(pool, "test") end

          assert_receive {[:logflare, :syslog_pool, :connect, :start], ^ref, _, metadata}
          assert_span_metadata(metadata, backend_id, context.config)

          assert_receive {[:logflare, :syslog_pool, :connect, :exception], ^ref, %{duration: _},
                          metadata}

          assert metadata.kind == :error
          assert metadata.reason == %RuntimeError{message: "connection failed"}
          assert is_list(metadata.stacktrace)

          assert_span_metadata(
            Map.drop(metadata, [:kind, :reason, :stacktrace]),
            backend_id,
            context.config
          )

          assert_logger_attached()
        end)

      assert log =~ "[error] [Syslog] Backend #{backend_id} failed to connect to 127.0.0.1:"
      assert log =~ "connection failed"
      refute log =~ @secret
    end
  end

  @spec assert_span_metadata(map(), pos_integer(), map(), map()) :: term()
  defp assert_span_metadata(metadata, backend_id, config, extra \\ %{}) do
    assert is_reference(metadata.telemetry_span_context)

    assert Map.delete(metadata, :telemetry_span_context) ==
             Map.merge(
               %{backend_id: backend_id, config: Map.take(config, [:host, :port, :tls])},
               extra
             )
  end

  @spec assert_logger_attached() :: term()
  defp assert_logger_attached do
    assert Enum.count(:telemetry.list_handlers([:logflare, :syslog_pool]), fn handler ->
             handler.id == "logflare-syslog-logger"
           end) == 4
  end
end
