defmodule LogflareTelemetry.Supervisor do
  @moduledoc "Main Logflare telemetry supervisor"
  use Supervisor
  alias Telemetry.Metrics
  alias LogflareTelemetry, as: LT
  alias LT.LogflareMetrics
  alias LT.MetricsCache

  def start_link(args \\ %{}) do
    Supervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(args) do
    # ecto_metrics = args[:ecto_metrics] || metrics(:ecto)
    ecto_metrics = metrics(:ecto)

    children = [
      {LT.Reporters.Ecto.V0, metrics: ecto_metrics},
      {LT.Reporters.BEAM.V0, metrics: metrics(:beam)},
      {LT.Aggregators.Ecto.V0, tick_interval: 1_000, metrics: ecto_metrics},
      {LT.Aggregators.BEAM.V0, tick_interval: 1_000, metrics: metrics(:beam)},
      MetricsCache
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def metrics(:ecto) do
    event_id = [:logflare, :repo, :query]
    measurement_names = ~w[decode_time query_time queue_time total_time]a

    measurement_names
    |> Enum.map(&[Metrics.summary(event_id ++ [&1])])
    |> Enum.concat([LogflareMetrics.all(event_id)])
    |> List.flatten()
  end

  def metrics(:beam) do
    vm_memory_measurements = [
      :atom,
      :atom_used,
      :binary,
      :code,
      :ets,
      :processes,
      :processes_used,
      :system,
      :total
    ]

    vm_total_run_queue_length_measurements = [:cpu, :io, :total]

    vm_memory_metrics =
      for m <- vm_memory_measurements do
        Metrics.last_value([:vm, :memory, m])
      end

    vm_total_run_queue_length_metrics =
      for m <- vm_total_run_queue_length_measurements do
        Metrics.last_value([:vm, :total_run_queue_lengths, m])
      end

    vm_memory_metrics ++ vm_total_run_queue_length_metrics
  end
end
