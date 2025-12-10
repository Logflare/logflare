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
      case Finch.get_pool_status(pool, url) do
        {:ok, metrics} ->
          counts =
            for metric <- metrics,
                Map.get(metric, :in_flight_requests),
                do: metric.in_flight_requests

          in_use_connections =
            for metric <- metrics,
                Map.get(metric, :in_use_connections),
                do: metric.in_use_connections

          available_connections =
            for metric <- metrics,
                Map.get(metric, :available_connections),
                do: metric.available_connections

          :telemetry.execute(
            [:logflare, :system, :finch],
            %{
              in_flight_requests: Enum.sum(counts),
              in_use_connections: Enum.sum(in_use_connections),
              available_connections: Enum.sum(available_connections)
            },
            %{url: url, pool: Atom.to_string(pool)}
          )

        _ ->
          nil
      end
    end
  end
end
