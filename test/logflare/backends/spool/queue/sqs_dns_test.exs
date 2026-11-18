defmodule Logflare.Backends.Spool.Queue.SQSDnsTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Logflare.TestUtilsDns

  alias Logflare.Backends.Spool.Health
  alias Logflare.Backends.Spool.Queue.SQS

  @queue_url "https://sqs.eu-west-1.amazonaws.com/123456789012/logflare-spool-prod-a-euw1-az1"
  @log_line ~s(ExAws: HTTP ERROR: %Mint.TransportError{reason: :nxdomain} for URL: "https://sqs.eu-west-1.amazonaws.com/" ATTEMPT: 1)

  setup do
    if is_nil(Process.whereis(Logflare.FinchSpoolSQS)) do
      start_supervised!({Finch, name: Logflare.FinchSpoolSQS})
    end

    ex_aws_env = Application.get_all_env(:ex_aws)
    spool_config = Application.get_env(:logflare, :spool, [])
    Application.put_env(:logflare, :spool, max_spool_health_failures: 1)

    Application.put_env(:ex_aws, :region, "eu-west-1")
    Application.put_env(:ex_aws, :access_key_id, "AKIDEXAMPLE")
    Application.put_env(:ex_aws, :secret_access_key, "secret")

    Application.put_env(:ex_aws, :retries,
      max_attempts: 2,
      base_backoff_in_ms: 1,
      max_backoff_in_ms: 1
    )

    on_exit(fn ->
      for {key, _} <- Application.get_all_env(:ex_aws), do: Application.delete_env(:ex_aws, key)
      for {key, value} <- ex_aws_env, do: Application.put_env(:ex_aws, key, value)

      Health.report_recovery!(:queue)

      Application.put_env(:logflare, :spool, spool_config)
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
      refute Health.healthy?(:queue)
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
    refute Health.healthy?(:queue)
  end
end
