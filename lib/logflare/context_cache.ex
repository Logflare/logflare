defmodule Logflare.ContextCache do
  @moduledoc """
  Read-through cache for hot database paths and/or functions. This module functions as the entry point for
  contexts to have a cache of function calls.

  e.g. `Logflare.Users.Cache` functions go through `apply_fun/3` and results of those
  functions are returned to the caller and cached in the respective cache.

  The cache implementation of `Logflare.ContextCache` is a reverse index where values
  returned by functions are used as the cache key.

  We must keep a reverse index because function are called by their arguments. So in the
  `Logflare.Users.Cache` we can keep a key of the MFA and a value of the results.

  But when a record from the write-ahead log comes in the `CacheBuster` calls `bust_keys/1`
  and we must know what the key is in the `Logflare.Users.Cache` to bust.

  So we keep the value of the `Logflare.Users.Cache` as the key in the `Logflare.ContextCache`
  and the value of our `Logflare.ContextCache` key is the key for our `Logflare.Users.Cache`.

  ## Memoization

  This module can also be used to cache heavy functions or db calls hidden behind a 3rd party
  library. See `Logflare.Auth.Cache` for an example. In this example, the `expiration` set in that
  Cachex child_spec is handling the cache expiration.

  In the case functions don't return a response with a primary key, or something else we can
  bust the cache on, it will get reverse indexed with `select_key/1` as `:unknown`.
  """

  require Logger

  @cache __MODULE__

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           @cache,
           [
             stats: stats,
             expiration:
               Cachex.Spec.expiration(
                 # default record expiration of 20 mins
                 default: :timer.minutes(20),
                 # how often cleanup should occur, 5 mins
                 interval: :timer.minutes(5),
                 # whether to enable lazy checking
                 lazy: true
               )
           ]
         ]}
    }
  end

  @spec apply_fun(atom(), tuple(), [list()]) :: any()
  def apply_fun(context, {fun, _arity}, args) do
    cache = cache_name(context)
    cache_key = {fun, args}

    case Cachex.fetch(cache, cache_key, fn {fun, args} ->
           # Use a `:cached` tuple here otherwise when an fn returns nil Cachex will miss
           # the cache because it thinks ETS returned nil
           {:commit, {:cached, apply(context, fun, args)}}
         end) do
      {:commit, {:cached, value}} ->
        keys_key = {{context, select_key(value)}, :erlang.phash2(cache_key)}
        Cachex.put(@cache, keys_key, cache_key)

        value

      {:ok, {:cached, value}} ->
        value
    end
  end

  @doc """
  This function is called from the CacheBuster process when a new record comes in from the Postgres
  write-ahead log. The WAL contains records. From those records the CacheBuster picks out
  primary keys.

  The records ARE the keys in the reverse cache (the ContextCache).

  We must:
   - Find the key by the record primary key
   - Delete the reverse cache entry
   - Delete the cache entry for that context cache e.g. `Logflare.Users.Cache`
  """

  @spec bust_keys(list()) :: {:ok, :busted}
  def bust_keys([]), do: {:ok, :busted}

  def bust_keys(values) when is_list(values) do
    for {context, primary_key} <- values do
      filter = {:==, {:element, 1, :key}, {{context, primary_key}}}
      query = Cachex.Query.create(filter, {:key, :value})
      context_cache = cache_name(context)

      Logflare.ContextCache
      |> Cachex.stream!(query)
      |> Enum.each(fn {k, v} ->
        Cachex.del(context_cache, v)
        Cachex.del(@cache, k)
      end)
    end

    {:ok, :busted}
  end

  @spec cache_name(atom()) :: atom()
  def cache_name(context) do
    Module.concat(context, Cache)
  end

  defp select_key(%_{id: id}), do: id
  defp select_key(true), do: "true"
  defp select_key(nil), do: :not_found
  defp select_key(_), do: :unknown
end
