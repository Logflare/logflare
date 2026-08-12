defmodule Logflare.Readiness do
  @moduledoc """
  Tracks whether this node should receive traffic.

  Readiness is stored outside the application supervision tree so signal handling
  can mark the node unready before the supervised processes begin shutting down.
  """

  @key {__MODULE__, :ready?}

  @spec ready?() :: boolean()
  def ready?, do: :persistent_term.get(@key, false)

  @spec mark_ready() :: :ok
  def mark_ready, do: :persistent_term.put(@key, true)

  @spec mark_unready() :: :ok
  def mark_unready, do: :persistent_term.put(@key, false)
end
