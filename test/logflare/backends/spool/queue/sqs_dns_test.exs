defmodule Logflare.Backends.Spool.Queue.SQSDnsTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Logflare.Backends.Spool.Queue.SQS

  @queue_url "https://sqs.eu-west-1.amazonaws.com/123456789012/logflare-spool-prod-a-euw1-az1"
  @log_line ~s(ExAws: HTTP ERROR: %Mint.TransportError{reason: :nxdomain} for URL: "https://sqs.eu-west-1.amazonaws.com/" ATTEMPT: 1)

  setup do
    if is_nil(Process.whereis(Logflare.FinchSpoolSQS)) do
      start_supervised!({Finch, name: Logflare.FinchSpoolSQS})
    end

    ex_aws_env = Application.get_all_env(:ex_aws)

    Application.put_env(:ex_aws, :region, "eu-west-1")
    Application.put_env(:ex_aws, :access_key_id, "AKIDEXAMPLE")
    Application.put_env(:ex_aws, :secret_access_key, "secret")

    Application.put_env(:ex_aws, :retries,
      max_attempts: 2,
      base_backoff_in_ms: 1,
      max_backoff_in_ms: 1
    )

    resolv_conf = :inet_db.res_option(:resolv_conf)
    lookup = :inet_db.res_option(:lookup)
    nameservers = :inet_db.res_option(:nameservers)
    timeout = :inet_db.res_option(:timeout)
    retry = :inet_db.res_option(:retry)

    on_exit(fn ->
      :inet_db.res_option(:resolv_conf, resolv_conf)
      :inet_db.set_lookup(lookup)
      :inet_db.res_option(:nameservers, nameservers)
      :inet_db.res_option(:timeout, timeout)
      :inet_db.res_option(:retry, retry)

      for {key, _} <- Application.get_all_env(:ex_aws), do: Application.delete_env(:ex_aws, key)
      for {key, value} <- ex_aws_env, do: Application.put_env(:ex_aws, key, value)
    end)

    :ok
  end

  for {mode, description} <- [
        drop: "the resolver never answers (dropped/throttled UDP)",
        servfail: "the resolver answers SERVFAIL",
        refused: "the resolver answers REFUSED",
        nxdomain: "the resolver answers NXDOMAIN"
      ] do
    test "publish reports :nxdomain when #{description}" do
      port = start_fake_dns(unquote(mode))
      use_resolver(port)

      log =
        capture_log(fn ->
          assert {:error, %Mint.TransportError{reason: :nxdomain}} =
                   SQS.publish(@queue_url, "payload")
        end)

      assert log =~ @log_line
      assert_received {:dns_query, ~c"sqs.eu-west-1.amazonaws.com"}
    end
  end

  test "publish reports :nxdomain when the resolver is not listening yet" do
    {:ok, socket} = :gen_udp.open(0, [:binary, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :gen_udp.close(socket)
    use_resolver(port)

    log =
      capture_log(fn ->
        assert {:error, %Mint.TransportError{reason: :nxdomain}} =
                 SQS.publish(@queue_url, "payload")
      end)

    assert log =~ @log_line
  end

  defp use_resolver(port) do
    :inet_db.res_option(:resolv_conf, [])
    :inet_db.set_lookup([:dns])
    :inet_db.res_option(:nameservers, [{{127, 0, 0, 1}, port}])
    :inet_db.res_option(:timeout, 100)
    :inet_db.res_option(:retry, 1)
  end

  defp start_fake_dns(mode) do
    test_pid = self()
    parent = self()

    pid =
      spawn_link(fn ->
        {:ok, socket} = :gen_udp.open(0, [:binary, ip: {127, 0, 0, 1}, active: true])
        {:ok, port} = :inet.port(socket)
        send(parent, {:dns_port, port})
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
