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
    case Finch.get_pool_status(Logflare.FinchDefault, "https://bigquery.googleapis.com") do
      {:ok, bq_metrics} ->
        for metric <- bq_metrics do
          :telemetry.execute(
            [:logflare, :system, :finch],
            %{in_flight_requests: metric.in_flight_requests},
            %{pool_index: metric.pool_index, url: "https://bigquery.googleapis.com"}
          )
        end

      _ ->
        :noop
    end
  end
end
