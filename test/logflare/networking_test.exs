defmodule Logflare.NetworkingTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.Networking

  describe "single tenant mode using Big Query" do
    TestUtils.setup_single_tenant()

    test "returns bigquery and clickhouse connection pools" do
      assert Enum.map(Networking.pools(), fn {Finch, opts} ->
               Keyword.get(opts, :name)
             end) == [
               Logflare.FinchGoth,
               Logflare.FinchDefaultHttp1,
               Logflare.FinchBQStorageWrite,
               Logflare.FinchIngest,
               Logflare.FinchQuery,
               Logflare.FinchDefault,
               Logflare.FinchClickhouseIngest
             ]
    end
  end

  describe "single tenant mode using Postgres" do
    TestUtils.setup_single_tenant(backend_type: :postgres)

    test "returns only datadog connection pools" do
      assert [
               {Finch,
                [
                  name: Logflare.FinchDefault,
                  pools: %{
                    :default => [protocols: [:http1]],
                    "https://http-intake.logs.ap1.datadoghq.com" => [
                      protocols: [:http1],
                      start_pool_metrics?: true
                    ],
                    "https://http-intake.logs.datadoghq.com" => [
                      protocols: [:http1],
                      start_pool_metrics?: true
                    ],
                    "https://http-intake.logs.datadoghq.eu" => [
                      protocols: [:http1],
                      start_pool_metrics?: true
                    ],
                    "https://http-intake.logs.us3.datadoghq.com" => [
                      protocols: [:http1],
                      start_pool_metrics?: true
                    ],
                    "https://http-intake.logs.us5.datadoghq.com" => [
                      protocols: [:http1],
                      start_pool_metrics?: true
                    ]
                  }
                ]},
               {Finch,
                name: Logflare.FinchClickhouseIngest,
                pools: %{
                  :default => _config
                }}
             ] = Networking.pools()
    end
  end
end
