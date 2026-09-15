defmodule Logflare.Backends.Spool.Health do
  @moduledoc """
  Tracks whether this node's spool can currently write and commit data.
  Self-healing: flips unhealthy after a run of consecutive failures
  (config `max_spool_health_failures`, default 3), and any success resets
  the counter and clears the unhealthy state. Emits
  `[:logflare, :backends, :spool, :write_health]` telemetry on every change.
  """

  @key {__MODULE__, :state}
  @healthy 1
  @unhealthy 0
  @healthy_index 1
  @failure_count_index 2
  @default_max_spool_health_failures 3

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
        if :atomics.add_get(state, @failure_count_index, 1) >= max_spool_health_failures() do
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

  defp max_spool_health_failures do
    Application.get_env(:logflare, :spool, [])
    |> Keyword.get(:max_spool_health_failures, @default_max_spool_health_failures)
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
