defmodule Logflare.BanditLoggerMetadataTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  test "correlates keepalive requests with the existing Bandit connection crash" do
    caller = self()

    server =
      start_supervised!(
        {Bandit,
         plug: fn conn, _opts ->
           send(caller, {:request, self(), Logger.metadata()[:connection_id]})
           Plug.Conn.send_resp(conn, 200, "ok")
         end,
         ip: {127, 0, 0, 1},
         port: 0,
         startup_log: false,
         thousand_island_options: [num_acceptors: 1]}
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

    {:ok, client} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], to_timeout(second: 1))

    :ok = :logger.add_handler(__MODULE__, __MODULE__, %{config: %{pid: self()}})

    on_exit(fn ->
      :gen_tcp.close(client)
      :logger.remove_handler(__MODULE__)
    end)

    capture_log([level: :error], fn ->
      requests =
        for path <- ["/first", "/second"] do
          :ok = :gen_tcp.send(client, "GET #{path} HTTP/1.1\r\nHost: localhost\r\n\r\n")
          receive_response(client)
          assert_receive {:request, pid, connection_id}
          :sys.get_state(pid)
          {:dictionary, dictionary} = Process.info(pid, :dictionary)
          refute Keyword.has_key?(dictionary, :"$logger_metadata$")
          {pid, connection_id}
        end

      [{pid, connection_id}, {pid, connection_id}] = requests
      assert is_binary(connection_id) and byte_size(connection_id) > 0
      {%ThousandIsland.Socket{socket: socket}, _state} = :sys.get_state(pid)
      monitor = Process.monitor(pid)
      send(pid, {:tcp_error, socket, :econnreset})
      assert_receive {:DOWN, ^monitor, :process, ^pid, :econnreset}

      assert_receive {:log_event,
                      %{level: :error, meta: %{pid: ^pid, connection_id: ^connection_id}} =
                        event}

      message = event |> Logger.Formatter.format_event(8192) |> IO.chardata_to_string()
      assert message =~ "** (stop) :econnreset"
      assert message =~ "127, 0, 0, 1"
      assert message =~ "Last message: {:tcp_error"

      refute_receive {:log_event, %{level: :error, meta: %{connection_id: ^connection_id}}}
    end)
  end

  def log(event, %{config: %{pid: pid}}), do: send(pid, {:log_event, event})

  defp receive_response(client, response \\ "") do
    case String.split(response, "\r\n\r\n", parts: 2) do
      [headers, "ok"] ->
        assert headers =~ "200 OK"

      _response ->
        {:ok, data} = :gen_tcp.recv(client, 0, to_timeout(second: 1))
        receive_response(client, response <> data)
    end
  end
end
