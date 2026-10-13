defmodule Logflare.Readiness do
  @moduledoc """
  Tracks whether this node should receive traffic.

  Readiness is stored outside the application supervision tree so signal handling
  can begin draining before the supervised processes shut down. Draining is a
  terminal state for an application lifecycle, so startup cannot make a draining
  node ready again.

  It also latches whether the node has passed its full health check, database
  included, at least once. `/health` runs the full check until then and checks
  only node-local liveness afterwards, so a load balancer with a single health
  check gates a booting node on the database without dropping a running node
  when the database is slow.
  """

  @key {__MODULE__, :state}
  @lifecycle 1
  @startup_checks 2
  @starting 0
  @ready 1
  @draining 2

  @spec initialize() :: :ok
  def initialize do
    state = :atomics.new(2, signed: false)
    :persistent_term.put(@key, state)
  end

  @spec ready?() :: boolean()
  def ready? do
    case :persistent_term.get(@key, nil) do
      nil -> false
      state -> :atomics.get(state, @lifecycle) == @ready
    end
  end

  @spec mark_ready() :: :ok
  def mark_ready do
    case :persistent_term.get(@key, nil) do
      nil ->
        :ok

      state ->
        _previous_state = :atomics.compare_exchange(state, @lifecycle, @starting, @ready)
        :ok
    end
  end

  @spec begin_draining() :: :ok
  def begin_draining do
    case :persistent_term.get(@key, nil) do
      nil -> :ok
      state -> :atomics.put(state, @lifecycle, @draining)
    end
  end

  @spec startup_checks_passed?() :: boolean()
  def startup_checks_passed? do
    case :persistent_term.get(@key, nil) do
      nil -> false
      state -> :atomics.get(state, @startup_checks) == 1
    end
  end

  @spec mark_startup_checks_passed() :: :ok
  def mark_startup_checks_passed do
    case :persistent_term.get(@key, nil) do
      nil -> :ok
      state -> :atomics.put(state, @startup_checks, 1)
    end
  end
end
