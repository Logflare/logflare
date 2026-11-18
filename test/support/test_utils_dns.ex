defmodule Logflare.TestUtilsDns do
  import ExUnit.Assertions, only: [flunk: 1]
  import ExUnit.Callbacks, only: [on_exit: 1]

  @type mode :: :drop | :servfail | :nxdomain | :refused

  @spec start_fake_dns(mode()) :: :inet.port_number()
  def start_fake_dns(mode) do
    test_pid = self()

    pid =
      spawn_link(fn ->
        {:ok, socket} = :gen_udp.open(0, [:binary, ip: {127, 0, 0, 1}, active: true])
        {:ok, port} = :inet.port(socket)
        send(test_pid, {:dns_port, port})
        serve_dns(socket, mode, test_pid)
      end)

    receive do
      {:dns_port, port} ->
        on_exit(fn -> Process.exit(pid, :kill) end)
        port
    after
      1_000 -> flunk("fake DNS server did not start")
    end
  end

  @spec use_resolver(:inet.port_number()) :: :ok
  def use_resolver(port) do
    prev =
      for option <- [:resolv_conf, :lookup, :nameservers, :timeout, :retry],
          do: {option, :inet_db.res_option(option)}

    on_exit(fn ->
      for {option, value} <- prev, do: :inet_db.res_option(option, value)
    end)

    :inet_db.res_option(:resolv_conf, [])
    :inet_db.res_option(:lookup, [:dns])
    :inet_db.res_option(:nameservers, [{{127, 0, 0, 1}, port}])
    :inet_db.res_option(:timeout, 100)
    :inet_db.res_option(:retry, 1)
    :ok
  end

  defp serve_dns(socket, mode, test_pid) do
    receive do
      {:udp, ^socket, ip, port, packet} ->
        {:ok, msg} = :inet_dns.decode(packet)

        for query <- :inet_dns.msg(msg, :qdlist) do
          send(test_pid, {:dns_query, :inet_dns.dns_query(query, :domain)})
        end

        case rcode(mode) do
          nil ->
            :ok

          rcode ->
            header = :inet_dns.make_header(:inet_dns.msg(msg, :header), qr: true, rcode: rcode)
            reply = :inet_dns.make_msg(msg, :header, header)
            :gen_udp.send(socket, ip, port, :inet_dns.encode(reply))
        end

        serve_dns(socket, mode, test_pid)
    end
  end

  defp rcode(:drop), do: nil
  defp rcode(:servfail), do: 2
  defp rcode(:nxdomain), do: 3
  defp rcode(:refused), do: 5
end
