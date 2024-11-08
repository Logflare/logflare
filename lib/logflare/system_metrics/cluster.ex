defmodule Logflare.SystemMetrics.Cluster do
  @moduledoc false
  require Logger
  alias Logflare.Cluster.Utils

  def dispatch_stats() do
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

  def finch() do
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
        ] do
      case Finch.get_pool_status(pool, url) do
        {:ok, metrics} ->
          counts = for metric <- metrics, do: metric.in_flight_requests

          :telemetry.execute(
            [:logflare, :system, :finch],
            %{in_flight_requests: Enum.sum(counts)},
            %{url: url, pool: Atom.to_string(pool)}
          )

        _ ->
          nil
      end
    end
  end
end
