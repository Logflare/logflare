defmodule Logflare.Backends.Adaptor.SyslogAdaptor.SocketTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Adaptor.SyslogAdaptor.Socket
  alias Logflare.SingleTenant
  alias Logflare.Utils.SSRF

  @ipv6_loopback {0, 0, 0, 0, 0, 0, 0, 1}

  setup :set_mimic_global
  setup :verify_on_exit!

  setup do
    stub(SingleTenant, :single_tenant?, fn -> false end)
    :ok
  end

  test "rejects private destinations before opening a connection" do
    {:ok, listener, port} = listen_tcp()

    assert {:error, {:ssrf, reason}} =
             Socket.connect(%{host: "127.0.0.1", port: port}, to_timeout(second: 1))

    assert reason =~ "private or reserved"
    assert {:error, :timeout} = :gen_tcp.accept(listener, 50)

    :ok = :gen_tcp.close(listener)
  end

  test "connects directly to the address returned by the safe resolver" do
    {:ok, listener, port} = listen_tcp()

    expect(SSRF, :safe_resolve_all, fn "syslog.invalid" -> {:ok, [{127, 0, 0, 1}]} end)

    assert {:ok, client} =
             Socket.connect(%{host: "syslog.invalid", port: port}, to_timeout(second: 1))

    assert {:ok, server} = :gen_tcp.accept(listener, 1_000)

    :ok = Socket.close(client)
    :ok = :gen_tcp.close(server)
    :ok = :gen_tcp.close(listener)
  end

  test "tries each safely resolved address until one connects" do
    host = "syslog-multi.test"
    first_address = non_loopback_ipv4_address()
    second_address = {127, 0, 0, 1}

    expect(SSRF, :safe_resolve_all, fn ^host ->
      {:ok, [first_address, second_address]}
    end)

    {:ok, listener, port} = listen_tcp()

    assert {:ok, client} =
             Socket.connect(%{host: host, port: port}, to_timeout(second: 1))

    assert {:ok, server} = :gen_tcp.accept(listener, 1_000)

    :ok = Socket.close(client)
    :ok = :gen_tcp.close(server)
    :ok = :gen_tcp.close(listener)
  end

  test "retains the original hostname for TLS SNI and hostname verification" do
    {:ok, listener, port, server} = listen_tls()

    expect(SSRF, :safe_resolve_all, fn "telegraf" -> {:ok, [@ipv6_loopback]} end)

    config = %{
      host: "telegraf",
      port: port,
      tls: true,
      ca_cert: File.read!("priv/telegraf/ca.crt")
    }

    assert {:ok, client} = Socket.connect(config, to_timeout(second: 1))
    assert_receive {:sni, ~c"telegraf"}
    assert_receive {:tls_server, {:ok, _server_socket}}

    :ok = Socket.close(client)
    send(server.pid, :close)
    assert :ok = Task.await(server)
    :ok = :ssl.close(listener)
  end

  test "allows private destinations only in explicitly configured single-tenant mode" do
    stub(SingleTenant, :single_tenant?, fn -> true end)
    stub(SSRF, :safe_resolve_all, fn _host -> flunk("safe resolver should be bypassed") end)

    {:ok, listener, port} = listen_tcp()

    assert {:ok, client} =
             Socket.connect(%{host: "127.0.0.1", port: port}, to_timeout(second: 1))

    assert {:ok, server} = :gen_tcp.accept(listener, 1_000)

    :ok = Socket.close(client)
    :ok = :gen_tcp.close(server)
    :ok = :gen_tcp.close(listener)
  end

  defp listen_tcp do
    {:ok, listener} = :gen_tcp.listen(0, active: false, ip: {127, 0, 0, 1}, reuseaddr: true)
    {:ok, {{127, 0, 0, 1}, port}} = :inet.sockname(listener)
    {:ok, listener, port}
  end

  defp non_loopback_ipv4_address do
    {:ok, interfaces} = :inet.getifaddrs()

    Enum.find_value(interfaces, fn {_name, options} ->
      Enum.find_value(options, fn
        {:addr, {first, _, _, _} = address} when first not in [0, 127] -> address
        _option -> nil
      end)
    end) || flunk("expected a non-loopback IPv4 interface")
  end

  defp listen_tls do
    test_pid = self()

    opts = [
      :inet6,
      active: false,
      certfile: String.to_charlist(Path.expand("priv/telegraf/server.crt")),
      keyfile: String.to_charlist(Path.expand("priv/telegraf/server.key")),
      ip: @ipv6_loopback,
      reuseaddr: true,
      sni_fun: fn hostname ->
        send(test_pid, {:sni, hostname})
        []
      end
    ]

    {:ok, listener} = :ssl.listen(0, opts)
    {:ok, {@ipv6_loopback, port}} = :ssl.sockname(listener)

    server =
      Task.async(fn ->
        result =
          with {:ok, socket} <- :ssl.transport_accept(listener, 1_000),
               {:ok, socket} <- :ssl.handshake(socket, 1_000) do
            {:ok, socket}
          end

        send(test_pid, {:tls_server, result})

        case result do
          {:ok, socket} ->
            receive do
              :close -> :ssl.close(socket)
            end

          {:error, _reason} ->
            :ok
        end
      end)

    {:ok, listener, port, server}
  end
end
