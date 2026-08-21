defmodule Logflare.NetworkingTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.Backends.Adaptor.DatadogAdaptor
  alias Logflare.Networking

  describe "multi-tenant mode" do
    setup :use_non_test_networking_config

    test "returns BigQuery, gRPC, and ClickHouse connection pools" do
      assert Logflare.FinchGoth in finch_names()
      assert Logflare.FinchIngest in finch_names()
      assert Logflare.FinchQuery in finch_names()
      assert Logflare.FinchClickHouseIngest in finch_names()

      assert {Logflare.Networking.GrpcPool, _opts} =
               Enum.find(Networking.pools(), &match?({Logflare.Networking.GrpcPool, _}, &1))
    end
  end

  describe "single tenant mode using Big Query" do
    TestUtils.setup_single_tenant()

    test "returns bigquery and clickhouse connection pools" do
      assert finch_names() == [
               Logflare.FinchGoth,
               Logflare.FinchDefaultHttp1,
               Logflare.FinchIngest,
               Logflare.FinchQuery,
               Logflare.FinchDefault,
               Logflare.FinchClickHouseIngest,
               Logflare.FinchClickHouseAsyncIngest
             ]
    end
  end

  describe "single tenant mode using Postgres" do
    TestUtils.setup_single_tenant(backend_type: :postgres)

    test "returns only datadog connection pools" do
      expected_datadog_pools =
        DatadogAdaptor.intake_origins()
        |> Map.new(fn origin ->
          {origin, [protocols: [:http1], start_pool_metrics?: true]}
        end)
        |> Map.put(:default, protocols: [:http1])

      assert [
               {Finch,
                [
                  name: Logflare.FinchDefault,
                  pools: datadog_pools
                ]},
               {Finch,
                name: Logflare.FinchClickHouseIngest,
                pools: %{
                  :default => _config
                }},
               {Finch,
                name: Logflare.FinchClickHouseAsyncIngest,
                pools: %{
                  :default => _async_config
                }}
             ] = Networking.pools()

      assert datadog_pools == expected_datadog_pools
    end
  end

  describe "single tenant mode using ClickHouse" do
    TestUtils.setup_single_tenant(backend_type: :clickhouse)
    setup :use_non_test_networking_config

    test "excludes BigQuery and gRPC connection pools" do
      assert finch_names() == [
               Logflare.FinchDefault,
               Logflare.FinchClickHouseIngest,
               Logflare.FinchClickHouseAsyncIngest
             ]

      refute Enum.any?(Networking.pools(), &match?({Logflare.Networking.GrpcPool, _}, &1))
    end
  end

  defp finch_names do
    Networking.pools()
    |> Enum.filter(fn
      {mod, _} -> mod == Finch
      _ -> false
    end)
    |> Enum.map(fn {Finch, opts} -> Keyword.get(opts, :name) end)
  end

  defp use_non_test_networking_config(_context) do
    previous_env = Application.get_env(:logflare, :env)
    Application.put_env(:logflare, :env, :dev)
    on_exit(fn -> Application.put_env(:logflare, :env, previous_env) end)
  end
end
