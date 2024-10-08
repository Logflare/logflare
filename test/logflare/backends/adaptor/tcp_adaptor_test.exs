defmodule Logflare.Backends.Adaptor.TCPAdaptorTest do
  use Logflare.DataCase

  @subject Logflare.Backends.Adaptor.TCPAdaptor

  doctest @subject

  setup do
    user = insert(:user)
    source = insert(:source, user_id: user.id)

    {port, socket} = listen()

    backend =
      insert(:backend,
        type: :tcp,
        sources: [source],
        config: %{host: "localhost", port: port, tls: false}
      )

    {:ok, source: source, backend: backend, port: port, socket: socket}
  end

  describe "ingest/3" do
    test "simple message", %{source: source, backend: backend} do
      le = build(:log_event, source: source)

      {:ok, pid} = @subject.start_link({source, backend})

      _ = @subject.ingest(pid, [le], [])

      assert_receive {:tcp, _msg}, 5000
    end

    test "message contains source ID", %{source: source, backend: backend} do
      le = build(:log_event, source: source)

      {:ok, pid} = @subject.start_link({source, backend})

      _ = @subject.ingest(pid, [le], [])

      assert_receive {:tcp, msg}, 5000

      assert msg =~ ~r/id="#{source.id}"/
    end
  end

  describe "telegraf" do
    @tag telegraf: true
    setup do
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      {:ok, port, tcp_port} = telegraf()

      backend =
        insert(:backend,
          type: :tcp,
          sources: [source],
          config: %{host: "localhost", port: tcp_port, tls: false}
        )

      {:ok, syslog_port: tcp_port, telegraf: port, backend: backend, source: source}
    end

    test "simple message", %{source: source, backend: backend, telegraf: port} do
      le = build(:log_event, source: source)

      {:ok, pid} = @subject.start_link({source, backend})

      _ = @subject.ingest(pid, [le], [])

      assert_receive {^port, {:data, {:eol, data}}}, 10_000
      content = Jason.decode!(data)
      assert "syslog" == content["name"]
    end
  end

  # Simple TCP server
  defp listen do
    this = self()

    spawn_link(fn ->
      {:ok, sock} =
        :gen_tcp.listen(0,
          mode: :binary,
          active: :once
        )

      {:ok, port} = :inet.port(sock)

      send(this, {port, sock})

      acceptor(sock, this)
    end)

    receive do
      {port, sock} -> {port, sock}
    end
  end

  defp acceptor(socket, parent) do
    {:ok, lsock} = :gen_tcp.accept(socket)
    ref = make_ref()

    pid =
      spawn_link(fn ->
        receive do
          ^ref -> server(lsock, parent)
        end
      end)

    :gen_tcp.controlling_process(lsock, pid)
    send(pid, ref)

    acceptor(socket, parent)
  end

  defp server(sock, pid) do
    receive do
      {:tcp_close, ^sock} ->
        :ok

      {:tcp, ^sock, msg} ->
        send(pid, {:tcp, msg})
        server(sock, pid)
    end
  end

  defp telegraf(options \\ []) do
    opts =
      Map.merge(
        %{
          framing: "octet-counting",
          port: 6789
        },
        Map.new(options)
      )

    env = [
      {~c"SYSLOG_PORT", to_charlist(opts.port)},
      {~c"SYSLOG_FRAMING", to_charlist(opts.framing)}
    ]

    wrapper = Path.expand("./test/support/syslog/run.sh")
    telegraf = System.find_executable("telegraf")

    port =
      Port.open(
        {:spawn_executable, to_charlist(wrapper)},
        [
          :binary,
          line: 16 * 1024,
          env: env,
          args: [telegraf, "--config", "test/support/syslog/telegraf.conf"]
        ]
      )

    Process.sleep(1000)

    {:ok, port, opts.port}
  end
end
