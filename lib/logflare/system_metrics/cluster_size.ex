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
end
