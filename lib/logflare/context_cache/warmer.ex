defmodule Logflare.ContextCache.Warmer do
  @moduledoc """
  Shared helpers for context cache warmers that periodically reload the most active
  records, so their entries are refreshed before they expire instead of on the request path.

  Warmers run every third of the cache TTL, with a jitter so that nodes started together
  don't query the database at the same time. Set `LOGFLARE_CACHE_WARMER_REFRESH_ENABLED=false`
  to run them only once, on startup.

  Entries whose records were invalidated while being loaded are dropped (see
  `Logflare.ContextCache.Gossip.recently_busted?/2`), so a warmer doesn't write back data
  that `Logflare.ContextCache.CacheBuster` has just removed.
  """

  require Logger

  alias Logflare.ContextCache.Gossip
  alias Logflare.Repo

  @jitter_ratio 0.1

  @type pairs :: [{term(), term()}]

  @doc """
  Returns the refresh interval for a cache with the given TTL in milliseconds, or `nil` when
  refreshing is disabled. The jitter is picked once, when the cache starts.
  """
  @spec interval(pos_integer()) :: pos_integer() | nil
  def interval(ttl) when is_integer(ttl) and ttl > 0 do
    if refresh_enabled?() do
      base = div(ttl, 3)
      jitter = round(base * @jitter_ratio)
      base + Enum.random(-jitter..jitter)
    end
  end

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

  defp refresh_enabled? do
    :logflare
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:refresh_enabled, false)
  end
end
