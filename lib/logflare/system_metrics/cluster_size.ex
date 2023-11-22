defmodule Logflare.SystemMetrics.Cluster do
  @modueldoc false
  require Logger
  alias Logflare.Cluster.Utils

  def dispatch_cluster_size() do
    # emit a telemetry event when called
    min = Utils.min_cluster_size()
    actual = Utils.actual_cluster_size()

    if actual < min do
      Logger.warning("Cluster size is #{actual} but expected #{min}",
        cluster_size: actual
      )
    end

    :telemetry.execute([:logflare, :cluster_size], %{count: actual}, %{min: min})
  end
end
