defmodule Logflare.Backends.Spool.WriteHealth do
  @moduledoc """
  Tracks whether this node can currently persist spool events durably
  anywhere — local disk WAL or, on a disk failure, the direct-to-GCS
  fallback (see `Logflare.Backends.Spool.Partition`).

  Unlike `Logflare.Readiness` (a one-way, terminal drain state for
  shutdown), this is a self-healing signal: `report_failure!/0` is only
  called once a partition has exhausted every way it knows to persist a
  request — a disk write failure alone doesn't call it, since the
  direct-to-GCS fallback might still succeed; only both failing does — and
  `report_recovery!/0` clears it again the moment anything succeeds
  afterward. `LogflareWeb.HealthCheckController` folds `healthy?/0` into
  its existing readiness checks, so a node that's genuinely lost both its
  local disk and its path to GCS fails its health check and stops
  receiving new traffic, rather than failing requests one at a time
  forever while still advertising itself as healthy.
  """

  @key {__MODULE__, :state}
  @healthy 1
  @unhealthy 0

  @spec initialize() :: :ok
  def initialize do
    state = :atomics.new(1, signed: false)
    :atomics.put(state, 1, @healthy)
    :persistent_term.put(@key, state)
  end

  @doc "Defaults to healthy if initialize/0 was never called (e.g. a node not running the spool producer at all)."
  @spec healthy?() :: boolean()
  def healthy? do
    case :persistent_term.get(@key, nil) do
      nil -> true
      state -> :atomics.get(state, 1) == @healthy
    end
  end

  @spec report_failure!() :: :ok
  def report_failure! do
    case :persistent_term.get(@key, nil) do
      nil -> :ok
      state -> :atomics.put(state, 1, @unhealthy)
    end
  end

  @spec report_recovery!() :: :ok
  def report_recovery! do
    case :persistent_term.get(@key, nil) do
      nil -> :ok
      state -> :atomics.put(state, 1, @healthy)
    end
  end
end
