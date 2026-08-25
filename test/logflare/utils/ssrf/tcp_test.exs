defmodule Logflare.Utils.SSRF.TCPTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Utils.SSRF
  alias Logflare.Utils.SSRF.TCP

  @ipv4_loopback {127, 0, 0, 1}
  @public_ipv4 {1, 1, 1, 1}
  @public_ipv6 {0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111}

  setup :set_mimic_global
  setup :verify_on_exit!

  describe "address resolution" do
    test "supports the configured OTP socket backend" do
      assert :ok = TCP.ensure_supported_backend!()
    end

    test "filters mixed IPv4 and IPv6 answers and interleaves the public addresses" do
      host = "safe-tcp-mixed.test"
      second_public_ipv4 = {8, 8, 8, 8}
      second_public_ipv6 = {0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888}

      put_host_addresses(host, [
        @ipv4_loopback,
        @public_ipv4,
        second_public_ipv4,
        @public_ipv6,
        {0, 0, 0, 0, 0, 0, 0, 1},
        second_public_ipv6
      ])

      expected = [@public_ipv4, @public_ipv6, second_public_ipv4, second_public_ipv6]

      assert {:ok, ^expected} = TCP.getaddrs(host)
      assert {:ok, ^expected} = TCP.getaddrs(String.to_charlist(host), false)
      assert {:ok, @public_ipv4} = TCP.getaddr(host)
      assert {:ok, @public_ipv4} = TCP.getaddr(String.to_charlist(host), false)
    end

    test "keeps the successful family when the other family does not resolve" do
      ipv4_host = "safe-tcp-ipv4-only.test"
      ipv6_host = "safe-tcp-ipv6-only.test"
      put_host_addresses(ipv4_host, [@public_ipv4])
      put_host_addresses(ipv6_host, [@public_ipv6])

      assert {:ok, [@public_ipv4]} = TCP.getaddrs(ipv4_host)
      assert {:ok, [@public_ipv6]} = TCP.getaddrs(ipv6_host)
    end

    test "returns access denied when every answer is blocked" do
      host = "safe-tcp-private.test"
      put_host_addresses(host, [@ipv4_loopback, {0, 0, 0, 0, 0, 0, 0, 1}])

      assert {:error, :eacces} = TCP.getaddrs(host)
      assert {:error, :eacces} = TCP.getaddr(host)
    end

    test "propagates the resolver error when neither family resolves" do
      use_file_lookup()

      assert {:error, :nxdomain} = TCP.getaddrs("safe-tcp-unresolved.test")
      assert {:error, :nxdomain} = TCP.getaddr("safe-tcp-unresolved.test")
    end

    test "blocks alternative textual representations of loopback" do
      for host <- [
            "127.1",
            "2130706433",
            "0177.0.0.1",
            "::ffff:127.0.0.1"
          ] do
        assert {:error, :eacces} = TCP.getaddrs(host), "expected block for #{host}"
      end

      assert TCP.getaddrs("0x7f000001") in [
               {:error, :eacces},
               {:error, :nxdomain}
             ]
    end

    test "returns public literal addresses" do
      assert {:ok, [@public_ipv4]} = TCP.getaddrs("1.1.1.1")
      assert {:ok, [@public_ipv6]} = TCP.getaddrs("2606:4700:4700::1111")
    end
  end

  describe "connection enforcement" do
    test "rechecks numeric addresses in each connect callback" do
      {:ok, listener, port} = listen_tcp()

      assert {:error, :eacces} = TCP.connect(@ipv4_loopback, port, [active: false], 200)

      socket_address = %{family: :inet, addr: @ipv4_loopback, port: port}
      assert {:error, :eacces} = TCP.connect(socket_address, [active: false], 200)

      assert {:error, :timeout} = :gen_tcp.accept(listener, 50)
      :ok = :gen_tcp.close(listener)
    end

    test "blocks a private literal before gen_tcp reaches the listener" do
      {:ok, listener, port} = listen_tcp()

      assert {:error, :eacces} =
               :gen_tcp.connect(
                 ~c"127.0.0.1",
                 port,
                 [{:tcp_module, TCP}, {:active, false}],
                 200
               )

      assert {:error, :timeout} = :gen_tcp.accept(listener, 50)
      :ok = :gen_tcp.close(listener)
    end

    test "blocks a private TLS destination before ssl reaches the listener" do
      {:ok, listener, port} = listen_tcp()

      assert {:error, :eacces} =
               :ssl.connect(
                 ~c"127.0.0.1",
                 port,
                 [{:tcp_module, TCP}, {:active, false}, {:verify, :verify_none}],
                 200
               )

      assert {:error, :timeout} = :gen_tcp.accept(listener, 50)
      :ok = :gen_tcp.close(listener)
    end

    test "tries the next approved address when the first connection fails" do
      allow_all_addresses()

      host = "safe-tcp-fallback.test"
      put_host_addresses(host, [non_loopback_ipv4_address(), @ipv4_loopback])
      {:ok, listener, port} = listen_tcp()

      assert {:ok, client} =
               :gen_tcp.connect(
                 String.to_charlist(host),
                 port,
                 [{:tcp_module, TCP}, {:active, false}],
                 1_000
               )

      assert {:ok, server} = :gen_tcp.accept(listener, 1_000)
      assert {:ok, {@ipv4_loopback, ^port}} = :inet.peername(client)

      :ok = :gen_tcp.close(client)
      :ok = :gen_tcp.close(server)
      :ok = :gen_tcp.close(listener)
    end

    test "preserves the original hostname for TLS SNI and certificate verification" do
      allow_all_addresses()

      put_host_addresses("telegraf", [@ipv4_loopback])
      {:ok, listener, port, server} = listen_tls()

      client_opts = [
        {:tcp_module, TCP},
        {:active, false},
        {:verify, :verify_peer},
        {:cacertfile, String.to_charlist(Path.expand("priv/telegraf/ca.crt"))},
        {:customize_hostname_check,
         [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]}
      ]

      assert {:ok, client} = :ssl.connect(~c"telegraf", port, client_opts, 1_000)
      assert_receive {:sni, ~c"telegraf"}
      assert_receive {:tls_server, {:ok, _server_socket}}

      :ok = :ssl.close(client)
      send(server.pid, :close)
      assert :ok = Task.await(server, 1_000)
      :ok = :ssl.close(listener)
    end
  end

  defp listen_tcp do
    {:ok, listener} =
      :gen_tcp.listen(0, active: false, ip: @ipv4_loopback, reuseaddr: true)

    {:ok, {@ipv4_loopback, port}} = :inet.sockname(listener)
    {:ok, listener, port}
  end

  defp allow_all_addresses do
    stub(SSRF, :private_ip?, fn _address -> false end)
    stub(SSRF, :public_addresses, fn addresses -> addresses end)
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
      active: false,
      certfile: String.to_charlist(Path.expand("priv/telegraf/server.crt")),
      keyfile: String.to_charlist(Path.expand("priv/telegraf/server.key")),
      ip: @ipv4_loopback,
      reuseaddr: true,
      sni_fun: fn hostname ->
        send(test_pid, {:sni, hostname})
        []
      end
    ]

    {:ok, listener} = :ssl.listen(0, opts)
    {:ok, {@ipv4_loopback, port}} = :ssl.sockname(listener)

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

  defp put_host_addresses(host, addresses) do
    use_file_lookup()
    hostname = String.to_charlist(host)

    Enum.each(addresses, &(:ok = :inet_db.add_host(&1, [hostname])))

    on_exit(fn ->
      Enum.each(addresses, &(:ok = :inet_db.del_host(&1)))
    end)
  end

  defp use_file_lookup do
    previous_lookup = :inet_db.res_option(:lookup)
    :ok = :inet_db.set_lookup([:file])
    on_exit(fn -> :ok = :inet_db.set_lookup(previous_lookup) end)
  end
end
