defmodule Logflare.UserMetrics do
  @moduledoc """
  Supervisor for the user metrics pipeline.

  Starts MetricStore (pull mode), registers telemetry handlers, and supervises
  the Broadway IngestPipeline that routes metrics into per-user system sources.
  """
  use Supervisor

  alias Logflare.Backends.UserMonitoring
  alias Logflare.Backends.UserMonitoring.IngestPipeline
  alias Logflare.UserMetrics.MetricStore
  alias Logflare.UserMetrics.TelemetryHandlers

  @store_name :user_metrics_store

  def store_name, do: @store_name

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    env = Application.get_env(:logflare, :env)

    {export_period, pull_interval, batch_timeout} =
      case env do
        :test -> {100, 100, 500}
        _ -> {:timer.minutes(8) + :rand.uniform(60_000 * 2), 1_000, 5_000}
      end

    store_config = %{
      metrics: UserMonitoring.metrics(),
      name: @store_name,
      export_period: export_period,
      pull_mode: true,
      hibernate_after: 5_000,
      spawn_opt: [fullsweep_after: 10_000]
    }

    handler_config = %{
      metrics: UserMonitoring.metrics(),
      store_name: @store_name,
      extract_tags: &UserMonitoring.extract_tags/2
    }

    pipeline_opts = [
      metric_store_name: @store_name,
      pull_interval: pull_interval,
      batch_timeout: batch_timeout
    ]

    children = [
      {MetricStore, store_config},
      {TelemetryHandlers, handler_config},
      {IngestPipeline, pipeline_opts}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
