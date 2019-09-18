defmodule LogflareTelemetry.Supervisor do
  @moduledoc "Main Logflare telemetry supervisor"
  use Supervisor
  alias Telemetry.Metrics
  alias LogflareTelemetry, as: LT
  alias LT.ExtendedMetrics, as: ExtMetrics
  alias LT.MetricsCache
  alias LT.Config
  @backend Logflare.TelemetryBackend.BQ

  def start_link(args \\ %{}) do
    Supervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(args) do
    config = args[:config] || default_config()

    children = [
      {LT.Reporters.V0.Ecto, config.ecto},
      {LT.Reporters.V0.BEAM, config.beam},
      {LT.Aggregators.V0.Ecto, config.ecto},
      {LT.Aggregators.V0.BEAM, config.beam},
      MetricsCache
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def default_config() do
    %Config{
      beam: %Config{
        metrics: metrics(:beam),
        tick_interval: 1_000,
        backend: @backend
      },
      ecto: %Config{metrics: metrics(:ecto), tick_interval: 1_000, backend: @backend}
    }
  end

  def metrics(:ecto) do
    event_id = [:logflare, :repo, :query]
    measurement_names = ~w[decode_time query_time queue_time total_time]a

    measurement_names
    |> Enum.map(&[Metrics.summary(event_id ++ [&1])])
    |> Enum.concat([ExtMetrics.every(event_id)])
    |> List.flatten()
  end

  def metrics(:beam) do
    # last atom is required to subscribe to the teleemetry events but is irrelevant as all measurements are collected
    vm_memory = [:vm, :memory]
    vm_total_run_queue_lengths = [:vm, :total_run_queue_lengths]

    [
      ExtMetrics.last_values(vm_memory),
      ExtMetrics.last_values(vm_total_run_queue_lengths)
    ]
  end

  # def metrics(:prev_beam) do
  #   vm_memory_measurements = [
  #     :atom,
  #     :atom_used,
  #     :binary,
  #     :code,
  #     :ets,
  #     :processes,
  #     :processes_used,
  #     :system,
  #     :total
  #   ]

  #   vm_total_run_queue_length_measurements = [:cpu, :io, :total]

  #   vm_memory_metrics =
  #     for m <- vm_memory_measurements do
  #       Metrics.last_value([:vm, :memory, m])
  #     end

  #   vm_total_run_queue_length_metrics =
  #     for m <- vm_total_run_queue_length_measurements do
  #       Metrics.last_value([:vm, :total_run_queue_lengths, m])
  #     end

  #   vm_memory_metrics ++ vm_total_run_queue_length_metrics
  # end
end
