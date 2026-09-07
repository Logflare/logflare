defmodule Logflare.NetworkingTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.Backends.Adaptor.DatadogAdaptor
  alias Logflare.Networking

  describe "single tenant mode using Big Query" do
    TestUtils.setup_single_tenant()

    test "returns bigquery and clickhouse connection pools" do
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
end
