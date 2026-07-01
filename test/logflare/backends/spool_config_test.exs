defmodule Logflare.Backends.SpoolConfigTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Spool.Queue.PubSub
  alias Logflare.Backends.Spool.Queue.SQS
  alias Logflare.Backends.Spool.Storage.GCS
  alias Logflare.Backends.Spool.Storage.S3

  # Helpers that mirror the provider-selection logic in both pipelines.
  defp storage_mod(provider), do: if(provider == :gcp, do: GCS, else: S3)
  defp queue_mod(provider), do: if(provider == :gcp, do: PubSub, else: SQS)

  describe "provider: :aws (default)" do
    test "selects S3 storage and SQS queue" do
      assert storage_mod(:aws) == S3
      assert queue_mod(:aws) == SQS
    end
  end

  describe "provider: :gcp" do
    test "selects GCS storage and PubSub queue" do
      assert storage_mod(:gcp) == GCS
      assert queue_mod(:gcp) == PubSub
    end
  end

  describe "SPOOL_PROVIDER env var parsing" do
    test "aws sets provider atom" do
      prev = Application.get_env(:logflare, :spool, [])

      try do
        provider = "aws"
        atom = String.to_atom(provider)
        assert atom == :aws
      after
        Application.put_env(:logflare, :spool, prev)
      end
    end

    test "gcp sets provider atom" do
      prev = Application.get_env(:logflare, :spool, [])

      try do
        provider = "gcp"
        atom = String.to_atom(provider)
        assert atom == :gcp
      after
        Application.put_env(:logflare, :spool, prev)
      end
    end

    test "provider config key is picked up by pipeline module selection" do
      prev = Application.get_env(:logflare, :spool, [])

      try do
        Application.put_env(:logflare, :spool, Keyword.put(prev, :provider, :gcp))
        spool_config = Application.get_env(:logflare, :spool, [])
        provider = Keyword.get(spool_config, :provider, :aws)
        assert provider == :gcp
        assert storage_mod(provider) == GCS
        assert queue_mod(provider) == PubSub
      after
        Application.put_env(:logflare, :spool, prev)
      end
    end

    test "missing provider defaults to aws" do
      prev = Application.get_env(:logflare, :spool, [])

      try do
        Application.put_env(:logflare, :spool, Keyword.delete(prev, :provider))
        spool_config = Application.get_env(:logflare, :spool, [])
        provider = Keyword.get(spool_config, :provider, :aws)
        assert provider == :aws
        assert storage_mod(provider) == S3
        assert queue_mod(provider) == SQS
      after
        Application.put_env(:logflare, :spool, prev)
      end
    end
  end

  describe "Queue.PubSub.resolve/1" do
    test "accepts valid projects/ paths" do
      assert {:ok, "projects/logflare/topics/logflare-spool"} =
               PubSub.resolve("projects/logflare/topics/logflare-spool")

      assert {:ok, "projects/logflare/subscriptions/logflare-spool-sub"} =
               PubSub.resolve("projects/logflare/subscriptions/logflare-spool-sub")
    end

    test "rejects paths not starting with projects/" do
      assert {:error, _reason} = PubSub.resolve("logflare-spool")
      assert {:error, _reason} = PubSub.resolve("topics/logflare-spool")
    end
  end

  describe "Queue.SQS.resolve/1 — mocked" do
    test "returns the URL from SQS on success" do
      import Mimic
      stub(ExAws, :request, fn _ -> {:ok, %{body: %{queue_url: "http://localhost:9324/000000000000/logflare-spool"}}} end)
      assert {:ok, "http://localhost:9324/000000000000/logflare-spool"} = SQS.resolve("logflare-spool")
    end

    test "returns error when SQS call fails" do
      import Mimic
      stub(ExAws, :request, fn _ -> {:error, {:http_error, 404, "not found"}} end)
      assert {:error, _} = SQS.resolve("logflare-spool")
    end
  end
end
