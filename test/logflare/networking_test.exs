defmodule Logflare.NetworkingTest do
  use Logflare.DataCase

  alias Logflare.Backends.Adaptor.DatadogAdaptor
  alias Logflare.Networking
  alias Logflare.Utils.SSRF.TCP

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
               Logflare.FinchSSRF,
               Logflare.FinchClickHouseIngest,
               Logflare.FinchClickHouseAsyncIngest
             ]
    end
  end

  describe "single tenant mode using Postgres" do
    TestUtils.setup_single_tenant(backend_type: :postgres)

    test "returns the Postgres-mode Finch pools" do
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
                name: Logflare.FinchSSRF,
                pools: %{
                  :default => ssrf_config
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
                }}
             ] = Networking.pools()

      assert datadog_pools == expected_datadog_pools

      assert ssrf_config[:protocols] == [:http1]
      assert ssrf_config[:conn_opts][:transport_opts][:inet6] == false
      assert ssrf_config[:conn_opts][:transport_opts][:tcp_module] == TCP
    end
  end
end
