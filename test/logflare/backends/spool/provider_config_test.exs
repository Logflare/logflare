defmodule Logflare.Backends.Spool.ProviderConfigTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Mimic

  alias Logflare.Backends.Spool.Health
  alias Logflare.Backends.Spool.ProviderConfig
  alias Logflare.Backends.Spool.Queue.PubSub
  alias Logflare.Backends.Spool.Queue.SQS
  alias Logflare.Backends.Spool.Storage.GCS
  alias Logflare.Backends.Spool.Storage.S3

  describe "resolve_mods/1" do
    test "defaults to S3/SQS when no provider is configured" do
      assert {S3, SQS} = ProviderConfig.resolve_mods([])
    end

    test "selects GCS/PubSub when provider is :gcp" do
      assert {GCS, PubSub} = ProviderConfig.resolve_mods(provider: :gcp)
    end

    test "selects S3/SQS when provider is :aws" do
      assert {S3, SQS} = ProviderConfig.resolve_mods(provider: :aws)
    end
  end

  describe "resolve_queue_ref/2" do
    test "uses queue_name for SQS even when a stale pubsub_topic is also configured" do
      test_pid = self()

      stub(ExAws, :request, fn %ExAws.Operation.Query{params: %{"QueueName" => name}}, _opts ->
        send(test_pid, {:queue_name_used, name})
        {:ok, %{body: %{queue_url: "http://localhost:9324/0/logflare-spool"}}}
      end)

      spool_config = [
        provider: :aws,
        pubsub_topic: "projects/logflare/topics/logflare-spool",
        queue_name: "logflare-spool"
      ]

      assert ProviderConfig.resolve_queue_ref(spool_config, SQS) ==
               "http://localhost:9324/0/logflare-spool"

      assert_received {:queue_name_used, "logflare-spool"}
    end

    test "uses pubsub_topic for PubSub even when a stale queue_name is also configured" do
      spool_config = [
        provider: :gcp,
        pubsub_topic: "projects/logflare/topics/logflare-spool",
        queue_name: "logflare-spool"
      ]

      assert ProviderConfig.resolve_queue_ref(spool_config, PubSub) ==
               "projects/logflare/topics/logflare-spool"
    end

    test "returns nil when neither key is configured" do
      assert ProviderConfig.resolve_queue_ref([], SQS) == nil
    end

    test "returns nil after logging each failed attempt, leaving readiness ungated" do
      test_pid = self()

      stub(ExAws, :request, fn _op, _opts ->
        send(test_pid, :resolve_attempt)
        {:error, %{reason: %Mint.TransportError{reason: :nxdomain}}}
      end)

      spool_config = [queue_name: "logflare-spool", queue_resolve_interval_ms: 0]

      log =
        capture_log(fn ->
          assert ProviderConfig.resolve_queue_ref(spool_config, SQS) == nil
        end)

      assert length(String.split(log, "could not resolve queue ref for logflare-spool")) == 7
      assert Health.healthy?(:queue)

      for _ <- 1..6, do: assert_received(:resolve_attempt)
      refute_received :resolve_attempt
    end
  end

  describe "resolve_queue/3" do
    test "retries until the queue resolves" do
      {:ok, attempts} = Agent.start_link(fn -> 0 end)

      stub(ExAws, :request, fn _op, _opts ->
        case Agent.get_and_update(attempts, &{&1 + 1, &1 + 1}) do
          n when n < 3 -> {:error, %{reason: %Mint.TransportError{reason: :nxdomain}}}
          _ -> {:ok, %{body: %{queue_url: "http://localhost:9324/0/logflare-spool"}}}
        end
      end)

      assert {:ok, "http://localhost:9324/0/logflare-spool"} =
               ProviderConfig.resolve_queue([queue_resolve_interval_ms: 0], SQS, "logflare-spool")

      assert Agent.get(attempts, & &1) == 3
      assert Health.healthy?(:queue)
    end

    test "waits the configured interval between attempts" do
      on_exit(fn -> Health.report_recovery!(:queue) end)
      stub(ExAws, :request, fn _op, _opts -> {:error, {:http_error, 404, "not found"}} end)

      spool_config = [queue_resolve_retries: 2, queue_resolve_interval_ms: 50]

      {elapsed_us, result} =
        :timer.tc(fn -> ProviderConfig.resolve_queue(spool_config, SQS, "logflare-spool") end)

      assert {:error, {:http_error, 404, "not found"}} = result
      assert elapsed_us >= 100_000
    end
  end
end
