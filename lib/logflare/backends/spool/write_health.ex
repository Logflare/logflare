defmodule Logflare.Backends.Spool.WriteHealth do
  @moduledoc """
  Tracks whether this node's local WAL disk is currently writable (see
  `Logflare.Backends.Spool.Partition`).

  Unlike `Logflare.Readiness` (a one-way, terminal drain state for
  shutdown), this is a self-healing signal, and it tolerates a run of
  `max_write_health_failures` (config, default 3) consecutive failures
  before flipping unhealthy — most individual write/roll/commit failures
  already get their own bounded retry inside `Partition`/`Committer`, so
  this is one more layer of tolerance against killing the node over
  something that turns out to be transient. `report_recovery!/0` resets
  the counter and clears the unhealthy state the moment anything succeeds.
  Every state change emits `[:logflare, :backends, :spool, :write_health]`
  telemetry (`healthy`: 1/0, `failure_count`) — see `Logflare.Telemetry` for
  the Grafana-visible gauges built from it. `Backends.spool_producer_mode?/0`
  already stops routing new events to the spool the moment this goes
  unhealthy (for the WAL buffer only — `:mem` has no local disk to be
  unhealthy about), independent of whatever
  `LogflareWeb.HealthCheckController` does with `healthy?/0` itself.
  """

  @key {__MODULE__, :state}
  @healthy 1
  @unhealthy 0
  @healthy_index 1
  @failure_count_index 2
  @default_max_write_health_failures 3

  @spec initialize() :: :ok
  def initialize do
    state = :atomics.new(2, signed: false)
    :atomics.put(state, @healthy_index, @healthy)
    :persistent_term.put(@key, state)
  end

  @doc "Defaults to healthy if initialize/0 was never called (e.g. a node not running the spool producer at all)."
  @spec healthy?() :: boolean()
  def healthy? do
    case :persistent_term.get(@key, nil) do
      nil -> true
      state -> :atomics.get(state, @healthy_index) == @healthy
    end
  end

  @spec report_failure!() :: :ok
  def report_failure! do
    case :persistent_term.get(@key, nil) do
      nil ->
        :ok

      state ->
        if :atomics.add_get(state, @failure_count_index, 1) >= max_write_health_failures() do
          :atomics.put(state, @healthy_index, @unhealthy)
        end

        emit_telemetry(state)
        :ok
    end
  end

  @spec report_recovery!() :: :ok
  def report_recovery! do
    case :persistent_term.get(@key, nil) do
      nil ->
        :ok

      state ->
        :atomics.put(state, @failure_count_index, 0)
        :atomics.put(state, @healthy_index, @healthy)
        emit_telemetry(state)
        :ok
    end
  end

  defp max_write_health_failures do
    Application.get_env(:logflare, :spool, [])
    |> Keyword.get(:max_write_health_failures, @default_max_write_health_failures)
  end

  defp emit_telemetry(state) do
    :telemetry.execute(
      [:logflare, :backends, :spool, :write_health],
      %{
        healthy: :atomics.get(state, @healthy_index),
        failure_count: :atomics.get(state, @failure_count_index)
      },
      %{}
    )
  end
end
