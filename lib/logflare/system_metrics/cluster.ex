defmodule Logflare.SystemMetrics.Cluster do
  @moduledoc false

  require Logger

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
    # TODO(ziinc): add in datadog pools
    for url <- [
          "https://bigquery.googleapis.com",
          "https://http-intake.logs.datadoghq.com",
          "https://http-intake.logs.us3.datadoghq.com",
          "https://http-intake.logs.us5.datadoghq.com",
          "https://http-intake.logs.datadoghq.eu",
          "https://http-intake.logs.ap1.datadoghq.com"
        ],
        pool <- [
          Logflare.FinchDefault,
          Logflare.FinchIngest,
          Logflare.FinchQuery
        ],
        GenServer.whereis(pool) != nil do
      dispatch_pool_telemetry(pool, url)
    end
  end

  @spec dispatch_pool_telemetry(module(), String.t()) :: :ok | nil
  defp dispatch_pool_telemetry(pool, url) do
    with {:ok, metrics} <- Finch.get_pool_status(pool, url) do
      measurements =
        Enum.reduce(
          metrics,
          %{in_flight_requests: 0, in_use_connections: 0, available_connections: 0},
          fn metric, acc ->
            %{
              in_flight_requests:
                acc.in_flight_requests + (Map.get(metric, :in_flight_requests) || 0),
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
    else
      _ -> nil
    end
  end
end
