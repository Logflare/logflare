defmodule Logflare.Backends.Adaptor.SyslogAdaptor.PoolTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.SyslogAdaptor.Pool
  alias Logflare.Backends.Adaptor.SyslogAdaptor.Socket

  test "closes a connection when its first checkout is cancelled" do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, packet: :raw, active: true, reuseaddr: true])

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

    stub(Socket, :send, fn _socket, _message ->
      send(test_pid, {:socket_send, self()})
      Process.sleep(:infinity)
    end)

    borrower = start_supervised!({Task, fn -> Pool.send(pool, "test") end})
    {:ok, peer} = :gen_tcp.accept(listen, to_timeout(second: 1))
    assert_receive {:socket_send, ^borrower}, to_timeout(second: 1)

    Process.exit(borrower, :kill)
    assert_receive {:tcp_closed, ^peer}, to_timeout(second: 1)
  end
end
