defmodule Logflare.Backends.Spool.ProviderConfigTest do
  use ExUnit.Case, async: true

  import Mimic

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
      stub(ExAws, :request, fn %ExAws.Operation.Query{params: %{"QueueName" => name}}, _opts ->
        assert name == "logflare-spool"
        {:ok, %{body: %{queue_url: "http://localhost:9324/0/logflare-spool"}}}
      end)

      spool_config = [
        provider: :aws,
        pubsub_topic: "projects/logflare/topics/logflare-spool",
        queue_name: "logflare-spool"
      ]

      assert ProviderConfig.resolve_queue_ref(spool_config, SQS) ==
               "http://localhost:9324/0/logflare-spool"
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

    test "returns nil when resolution fails" do
      stub(ExAws, :request, fn _op, _opts -> {:error, {:http_error, 404, "not found"}} end)

      assert ProviderConfig.resolve_queue_ref([queue_name: "logflare-spool"], SQS) == nil
    end
  end
end
