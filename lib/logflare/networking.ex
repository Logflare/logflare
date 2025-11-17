defmodule Logflare.Networking do
  @moduledoc false

  alias Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient
  alias Logflare.SingleTenant

  def pools do
    finch_pools(SingleTenant.postgres_backend?())
  end

  defp finch_pools(true = _postgres_backend?) do
    [
      {Finch,
       name: Logflare.FinchDefault,
       pools:
         %{
           # default pool uses finch defaults
           :default => [protocols: [:http1]]
         }
         |> Map.merge(datadog_connection_pools())}
      | base_finch_pools()
    ]
  end

  defp finch_pools(false = _postgres_backend?) do
    base = System.schedulers_online()
    http1_count = max(div(base, 4), 1)

    [
      # Finch connection pools, using http2
      {Finch, name: Logflare.FinchGoth, pools: %{default: [protocols: [:http2], count: 1]}},
      {Finch,
       name: Logflare.FinchDefaultHttp1, pools: %{default: [protocols: [:http1], size: 50]}},
      {Finch,
       name: GoogleApiClient.get_finch_instance_name(),
       pools: %{
         "https://bigquerystorage.googleapis.com" => [
           protocols: [:http2],
           count: max(base, 20),
           start_pool_metrics?: true,
           conn_opts: [
             # a larger default window size ensures that the number of packages exchanges is smaller, thus speeding up the requests
             # by reducing the amount of networks round trip, with the cost of having larger packages reaching the server per connection.
             client_settings: [
               initial_window_size: 8_000_000,
               max_frame_size: 8_000_000
             ]
           ]
         ]
       }},
      {Finch,
       name: Logflare.FinchIngest,
       pools: %{
         :default => [size: 50],
         "https://bigquery.googleapis.com" => [
           protocols: [:http1],
           size: max(base * 150, 150),
           count: http1_count,
           start_pool_metrics?: true
         ]
       }},
      {Finch,
       name: Logflare.FinchQuery,
       pools: %{
         "https://bigquery.googleapis.com" => [
           protocols: [:http2],
           count: max(base, 20) * 2,
           start_pool_metrics?: true
         ]
       }},
      {Finch,
       name: Logflare.FinchDefault,
       pools:
         %{
           # default pool uses finch defaults
           :default => [protocols: [:http1]],
           #  explicitly set http2 for other pools for multiplexing
           "https://bigquery.googleapis.com" => [
             protocols: [:http1],
             size: 115,
             count: http1_count,
             start_pool_metrics?: true
           ]
         }
         |> Map.merge(datadog_connection_pools())}
      | base_finch_pools()
    ]
  end

  defp base_finch_pools do
    base = System.schedulers_online()
    http1_count = max(div(base, 4), 1)

    [
      {Finch,
       name: Logflare.FinchClickhouseIngest,
       pools: %{
         default: [
           protocols: [:http1],
           size: max(base * 125, 150),
           count: http1_count,
           start_pool_metrics?: true
         ]
       }}
    ]
  end

  def datadog_connection_pools do
    providers = Application.get_env(:logflare, :http_connection_pools, ["all"])

    cond do
      "all" in providers ->
        # Explicitly provision all DataDog pools
        all_datadog_pools()

      "datadog" in providers ->
        # DataDog is explicitly listed
        all_datadog_pools()

      true ->
        # DataDog not in the list, don't include DataDog pools
        %{}
    end
  end

  defp all_datadog_pools do
    %{
      "https://http-intake.logs.datadoghq.com" => [
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
      ],
      "https://http-intake.logs.datadoghq.eu" => [
        protocols: [:http1],
        start_pool_metrics?: true
      ],
      "https://http-intake.logs.ap1.datadoghq.com" => [
        protocols: [:http1],
        start_pool_metrics?: true
      ]
    }
  end
end
