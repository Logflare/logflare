defmodule Logflare.NetworkingTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.Backends.Adaptor.DatadogAdaptor
  alias Logflare.Networking

  describe "single tenant mode using Big Query" do
    TestUtils.setup_single_tenant()

    setup do
      prev = Application.get_env(:logflare, :spool, [])
      Application.put_env(:logflare, :spool, Keyword.put(prev, :mode, :both))
      on_exit(fn -> Application.put_env(:logflare, :spool, prev) end)
      :ok
    end

    test "returns bigquery, clickhouse, and spool connection pools" do
      finch_names =
        Networking.pools()
        |> Enum.filter(fn
          {mod, _} -> mod == Finch
          _ -> false
        end)
        |> Enum.map(fn {Finch, opts} -> Keyword.get(opts, :name) end)

      assert finch_names == [
               Logflare.FinchGoth,
               Logflare.FinchDefaultHttp1,
               Logflare.FinchIngest,
               Logflare.FinchQuery,
               Logflare.FinchDefault,
               Logflare.FinchSpoolS3,
               Logflare.FinchSpoolSQS,
               Logflare.FinchClickHouseIngest,
               Logflare.FinchClickHouseAsyncIngest,
               Logflare.FinchS3
             ]
    end
  end

  describe "single tenant mode using Postgres" do
    TestUtils.setup_single_tenant(backend_type: :postgres)

    setup do
      prev = Application.get_env(:logflare, :spool, [])
      Application.put_env(:logflare, :spool, Keyword.put(prev, :mode, :both))
      on_exit(fn -> Application.put_env(:logflare, :spool, prev) end)
      :ok
    end

    test "returns bigquery, clickhouse, and spool connection pools" do
      expected_datadog_pools =
        DatadogAdaptor.intake_origins()
        |> Map.new(fn origin ->
          {origin, [protocols: [:http1], start_pool_metrics?: true]}
        end)
        |> Map.put(:default, protocols: [:http1])

      assert [
               {Finch,
                name: Logflare.FinchDefaultHttp1,
                pools: %{default: [protocols: [:http1], size: 50]}},
               {Finch,
                [
                  name: Logflare.FinchDefault,
                  pools: datadog_pools
                ]},
               {Finch,
                name: Logflare.FinchSpoolS3,
                pools: %{
                  default: _spool_s3_config
                }},
               {Finch,
                name: Logflare.FinchSpoolSQS,
                pools: %{
                  default: _spool_sqs_config
                }},
               {Finch,
                name: Logflare.FinchClickHouseIngest,
                pools: %{
                  :default => _config
                }},
               {Finch,
                name: Logflare.FinchClickHouseAsyncIngest,
                pools: %{
                  :default => _async_config
                }},
               {Finch,
                name: Logflare.FinchS3,
                pools: %{
                  :default => [
                    protocols: [:http1],
                    conn_max_idle_time: 5_000,
                    start_pool_metrics?: true,
                    conn_opts: [
                      transport_opts: [
                        timeout: 5_000,
                        send_timeout: 30_000,
                        send_timeout_close: true
                      ]
                    ]
                  ]
                }}
             ] = Networking.pools()

      assert datadog_pools == expected_datadog_pools
    end
  end

  describe "spool provider selection" do
    setup do
      prev = Application.get_env(:logflare, :spool, [])
      on_exit(fn -> Application.put_env(:logflare, :spool, prev) end)
      :ok
    end

    defp finch_names, do: Enum.flat_map(Networking.pools(), &finch_name/1)
    defp finch_name({Finch, opts}), do: [Keyword.fetch!(opts, :name)]
    defp finch_name(_), do: []

    test "starts only FinchSpool (GCS+Pub/Sub) when provider is :gcp" do
      Application.put_env(:logflare, :spool, provider: :gcp, mode: :both)

      names = finch_names()
      assert Logflare.FinchSpool in names
      refute Logflare.FinchSpoolS3 in names
      refute Logflare.FinchSpoolSQS in names
    end

    test "starts separate FinchSpoolS3 and FinchSpoolSQS pools when provider is :aws" do
      Application.put_env(:logflare, :spool, provider: :aws, mode: :both)

      names = finch_names()
      assert Logflare.FinchSpoolS3 in names
      assert Logflare.FinchSpoolSQS in names
      refute Logflare.FinchSpool in names
    end

    test "defaults to FinchSpoolS3 + FinchSpoolSQS when no provider is configured" do
      Application.put_env(:logflare, :spool, mode: :both)

      names = finch_names()
      assert Logflare.FinchSpoolS3 in names
      assert Logflare.FinchSpoolSQS in names
      refute Logflare.FinchSpool in names
    end

    test "starts no spool pools when spool mode is :disable, regardless of provider" do
      Application.put_env(:logflare, :spool, provider: :aws, mode: :disable)

      names = finch_names()
      refute Logflare.FinchSpoolS3 in names
      refute Logflare.FinchSpoolSQS in names
      refute Logflare.FinchSpool in names
    end

    test "starts no spool pools when spool mode is not configured (defaults to disabled)" do
      Application.put_env(:logflare, :spool, provider: :gcp)

      names = finch_names()
      refute Logflare.FinchSpoolS3 in names
      refute Logflare.FinchSpoolSQS in names
      refute Logflare.FinchSpool in names
    end

    test "starts spool pools for :producer-only and :consumer-only modes too" do
      Application.put_env(:logflare, :spool, provider: :aws, mode: :producer)
      assert Logflare.FinchSpoolS3 in finch_names()

      Application.put_env(:logflare, :spool, provider: :aws, mode: :consumer)
      assert Logflare.FinchSpoolS3 in finch_names()
    end
  end

  describe "S3 drain pool" do
    test "expires idle drain connections and enables pool metrics without tuning spool pools" do
      previous = Application.get_env(:logflare, :spool, [])
      Application.put_env(:logflare, :spool, provider: :aws, mode: :both)
      on_exit(fn -> Application.put_env(:logflare, :spool, previous) end)

      pools =
        for {Finch, opts} <- Networking.pools(), into: %{} do
          {Keyword.fetch!(opts, :name), Keyword.fetch!(opts, :pools)}
        end

      drain_config = pools[Logflare.FinchS3].default
      assert Keyword.fetch!(drain_config, :conn_max_idle_time) == 5_000
      assert Keyword.fetch!(drain_config, :start_pool_metrics?) == true
      assert Keyword.fetch!(drain_config, :protocols) == [:http1]

      for name <- [Logflare.FinchSpoolS3, Logflare.FinchSpoolSQS] do
        refute Keyword.has_key?(pools[name].default, :conn_max_idle_time)
      end
    end
  end

  describe "ClickHouse ingest pools" do
    test "bound the request send path in addition to connect" do
      for name <- [Logflare.FinchClickHouseIngest, Logflare.FinchClickHouseAsyncIngest] do
        assert {Finch, opts} =
                 Enum.find(Networking.pools(), fn
                   {Finch, opts} -> Keyword.get(opts, :name) == name
                   _ -> false
                 end)

        transport_opts =
          opts
          |> Keyword.fetch!(:pools)
          |> Map.fetch!(:default)
          |> Keyword.fetch!(:conn_opts)
          |> Keyword.fetch!(:transport_opts)

        assert Keyword.fetch!(transport_opts, :timeout) == :timer.seconds(10)
        assert Keyword.fetch!(transport_opts, :send_timeout) == :timer.seconds(15)
        assert Keyword.fetch!(transport_opts, :send_timeout_close) == true
      end
    end
  end
end
