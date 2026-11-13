defmodule Logflare.ContextCache.Warmer do
  @moduledoc """
  Shared loading logic for context cache warmers. `Logflare.Cache.CachexOps` re-runs warmers
  every third of the cache TTL, so their entries are refreshed before they expire instead of on
  the request path.

  Entries whose records were invalidated while being loaded are dropped (see
  `Logflare.ContextCache.Gossip.recently_busted?/2`), so a warmer doesn't write back data
  that `Logflare.ContextCache.CacheBuster` has just removed.
  """

  require Logger

  alias Logflare.ContextCache.Gossip
  alias Logflare.Repo

  @type pairs :: [{term(), term()}]

  @doc """
  Loads entries with `load` on a read replica and drops the ones whose records were recently
  invalidated. Errors are logged and return `:ignore`, so a failed run doesn't stop the next
  scheduled one.
  """
  @spec warm(atom(), (-> pairs())) :: {:ok, pairs()} | :ignore
  def warm(cache, load) when is_function(load, 0) do
    :telemetry.span([:logflare, :context_cache, :warm], %{cache: cache}, fn ->
      {pairs, stale} =
        load
        |> Repo.with_replica()
        |> Enum.split_with(fn {_key, value} -> not Gossip.recently_busted?(cache, value) end)

      {{:ok, pairs}, %{cache: cache, count: length(pairs), dropped: length(stale)}}
    end)
  rescue
    e ->
      Logger.error("Error warming #{inspect(cache)}: #{Exception.message(e)}")
      :ignore
  end
end
