defmodule Logflare.SystemMetrics.Cluster do
  @moduledoc false

  require Logger

  alias Logflare.Backends.Adaptor.DatadogAdaptor
  alias Logflare.Cluster.Utils

  def dispatch_stats do
    # emit a telemetry event when called
    min = Utils.min_cluster_size()
    actual = Utils.actual_cluster_size()

    if actual < min do
      Logger.warning("Cluster size is #{actual} but expected #{min}",
        cluster_size: actual
      )
    end

    :telemetry.execute([:logflare, :system, :cluster_size], %{count: actual}, %{min: min})
  end

  def finch do
    dispatch_s3_pool_telemetry()

    for url <- ["https://bigquery.googleapis.com" | DatadogAdaptor.intake_origins()],
        pool <- [
          Logflare.FinchDefault,
          Logflare.FinchIngest,
          Logflare.FinchQuery
        ],
        GenServer.whereis(pool) != nil do
      dispatch_pool_telemetry(pool, url)
    end
  end

  @spec dispatch_s3_pool_telemetry() :: :ok | nil
  defp dispatch_s3_pool_telemetry do
    pool = Logflare.FinchS3

    if GenServer.whereis(pool) != nil do
      metrics =
        pool
        |> Registry.select([{{:"$1", :_, Finch.HTTP1.Pool}, [], [:"$1"]}])
        |> Enum.uniq()
        |> Enum.flat_map(&s3_pool_metrics/1)

      dispatch_pool_metrics(pool, "all", metrics)
    end
  end

  @spec s3_pool_metrics(Finch.scheme_host_port()) :: [map()]
  defp s3_pool_metrics(origin) do
    case Finch.get_pool_status(Logflare.FinchS3, origin) do
      {:ok, metrics} -> metrics
      {:error, :not_found} -> []
    end
  end

  @spec dispatch_pool_telemetry(module(), String.t()) :: :ok | nil
  defp dispatch_pool_telemetry(pool, url) do
    with {:ok, metrics} <- Finch.get_pool_status(pool, url) do
      dispatch_pool_metrics(pool, url, metrics)
    else
      _ -> nil
    end
  end

  @spec dispatch_pool_metrics(module(), String.t(), [map()]) :: :ok
  defp dispatch_pool_metrics(pool, url, metrics) do
    measurements =
      Enum.reduce(
        metrics,
        %{in_flight_requests: 0, in_use_connections: 0, available_connections: 0},
        fn metric, acc ->
          in_flight_requests =
            Map.get(metric, :in_flight_requests) || Map.get(metric, :in_use_connections) || 0

          %{
            in_flight_requests: acc.in_flight_requests + in_flight_requests,
            in_use_connections:
              acc.in_use_connections + (Map.get(metric, :in_use_connections) || 0),
            available_connections:
              acc.available_connections + (Map.get(metric, :available_connections) || 0)
          }
        end
      )

    :telemetry.execute(
      [:logflare, :system, :finch],
      measurements,
      %{url: url, pool: Atom.to_string(pool)}
    )
  end
end
