defmodule Logflare.Readiness do
  @moduledoc """
  Tracks whether this node should receive traffic.

  Readiness is stored outside the application supervision tree so signal handling
  can begin draining before the supervised processes shut down. Draining is a
  terminal state for an application lifecycle, so startup cannot make a draining
  node ready again.
  """

  @key {__MODULE__, :state}
  @starting 0
  @ready 1
  @draining 2

  @spec initialize() :: :ok
  def initialize do
    state = :atomics.new(1, signed: false)
    :persistent_term.put(@key, state)
  end

  @spec ready?() :: boolean()
  def ready? do
    case :persistent_term.get(@key, nil) do
      nil -> false
      state -> :atomics.get(state, 1) == @ready
    end
  end

  @spec mark_ready() :: :ok
  def mark_ready do
    case :persistent_term.get(@key, nil) do
      nil ->
        :ok

      state ->
        _previous_state = :atomics.compare_exchange(state, 1, @starting, @ready)
        :ok
    end
  end

  @spec begin_draining() :: :ok
  def begin_draining do
    case :persistent_term.get(@key, nil) do
      nil -> :ok
      state -> :atomics.put(state, 1, @draining)
    end
  end
end
