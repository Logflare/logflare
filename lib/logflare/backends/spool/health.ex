defmodule Logflare.Backends.Spool.Health do
  @moduledoc """
  Tracks whether this node's spool can currently write and commit data,
  as two independently-tracked scopes: `:disk` (local WAL fsync, `:wal`
  buffer mode only) and `:upload` (the Cloud backend's GCS/queue commit,
  used directly in `:mem` mode and via `RotatingWal`'s workers in `:wal`
  mode). Keeping them separate means a `:wal`-mode node's frequent local
  fsync successes can't mask a persistently failing upload path.

  Self-healing per scope: a scope flips unhealthy after a run of
  consecutive failures (config `max_spool_health_failures`, default 3),
  and any success on that scope resets its counter and clears its
  unhealthy state. Emits `[:logflare, :backends, :spool, :write_health]`
  telemetry, tagged with `scope`, on every change.
  """

  @scopes [:disk, :upload]
  @healthy 1
  @unhealthy 0
  @healthy_index 1
  @failure_count_index 2
  @default_max_spool_health_failures 3

  @spec initialize() :: :ok
  def initialize do
    for scope <- @scopes do
      state = :atomics.new(2, signed: false)
      :atomics.put(state, @healthy_index, @healthy)
      :persistent_term.put(key(scope), state)
    end

    :ok
  end

  @doc "Defaults to healthy if initialize/0 was never called (e.g. a node not running the spool producer at all)."
  @spec healthy?(:disk | :upload) :: boolean()
  def healthy?(scope) do
    case :persistent_term.get(key(scope), nil) do
      nil -> true
      state -> :atomics.get(state, @healthy_index) == @healthy
    end
  end

  @spec report_failure!(:disk | :upload) :: :ok
  def report_failure!(scope) do
    case :persistent_term.get(key(scope), nil) do
      nil ->
        :ok

      state ->
        if :atomics.add_get(state, @failure_count_index, 1) >= max_spool_health_failures() do
          :atomics.put(state, @healthy_index, @unhealthy)
        end

        emit_telemetry(scope, state)
        :ok
    end
  end

  @spec report_recovery!(:disk | :upload) :: :ok
  def report_recovery!(scope) do
    case :persistent_term.get(key(scope), nil) do
      nil ->
        :ok

      state ->
        :atomics.put(state, @failure_count_index, 0)
        :atomics.put(state, @healthy_index, @healthy)
        emit_telemetry(scope, state)
        :ok
    end
  end

  defp key(scope), do: {__MODULE__, :state, scope}

  defp max_spool_health_failures do
    Application.get_env(:logflare, :spool, [])
    |> Keyword.get(:max_spool_health_failures, @default_max_spool_health_failures)
  end

  defp emit_telemetry(scope, state) do
    :telemetry.execute(
      [:logflare, :backends, :spool, :write_health],
      %{
        healthy: :atomics.get(state, @healthy_index),
        failure_count: :atomics.get(state, @failure_count_index)
      },
      %{scope: scope}
    )
  end
end
