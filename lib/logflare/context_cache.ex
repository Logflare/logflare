defmodule Logflare.ContextCache do
  @moduledoc """
  Read-through cache for hot database paths and/or functions. This module functions as the entry point for
  contexts to have a cache of function calls.

  e.g. `Logflare.Users.Cache` functions go through `apply_fun/3` and results of those
  functions are returned to the caller and cached in the respective cache.

  ## Implementation

  A context cache implements both this behaviour and `Logflare.Cache`. Every callback is optional;
  `Logflare.Cache.CachexOps` handles the ones a cache does not implement.

  ## Busting

  `bust_keys/1` busts entries by primary key or by a keyword of fields. Caches that need busting
  keys other than `id:` implement `c:keys_to_bust/1` themselves.

  ## Memoization

  This module can also be used to cache heavy functions or db calls hidden behind a 3rd party
  library. See `Logflare.Auth.Cache` for an example, where the cache's own expiration is the only
  invalidation.
  """

  @doc """
  Returns the keys of the entries to bust for a keyword of values.
  """
  @callback keys_to_bust(keyword()) :: Enumerable.t()

  @doc """
  Deletes `keys`, returning the number of entries deleted.
  """
  @callback delete_keys(Enumerable.t()) :: {:ok, non_neg_integer()}

  @doc """
  Returns the cached value for `key`, calling `getter` and caching its result on a miss.
  """
  @callback fetch(key :: term(), getter :: (-> term())) :: term()

  @doc """
  Replaces the value cached for `key`. Does nothing when `key` is not cached.
  """
  @callback update(key :: term(), value :: term()) :: :ok

  @optional_callbacks keys_to_bust: 1, delete_keys: 1, fetch: 2, update: 2

  @spec apply_fun(module(), tuple() | atom(), list()) :: any()
  def apply_fun(context, {fun, _arity}, args), do: apply_fun(context, fun, args)

  def apply_fun(context, fun, args) when is_atom(fun) do
    getter = fn -> Logflare.Repo.apply_with_replica(context, fun, args) end
    Logflare.Cache.dispatch(cache_name(context), :fetch, [{fun, args}, getter])
  end

  @doc """
  Updates cache entry to the given value
  """
  @spec update(module(), atom(), list(), term()) :: :ok
  def update(context, fun, args, value) when is_atom(fun) do
    Logflare.Cache.dispatch(cache_name(context), :update, [{fun, args}, value])
  end

  @doc """
  Busts cache entries based on context-primary-key pairs.

  It is intended for following a WAL for cache busting. When a new record comes in from the WAL,
  the CacheBuster process calls this function with either the primary keys extracted from those records
  or a keyword list with fields useful for busting. Both are passed to the context cache's
  `c:keys_to_bust/1`, a primary key as `[id: pkey]`, and the returned keys to `c:delete_keys/1`.
  """
  @spec bust_keys(list()) :: {:ok, non_neg_integer()}
  def bust_keys(values) when is_list(values) do
    busted =
      for {context, pkey_or_kw} <- values, reduce: 0 do
        acc ->
          {:ok, n} = bust_key(context, pkey_or_kw)
          acc + n
      end

    {:ok, busted}
  end

  defp bust_key(context, kw) when is_list(kw) do
    cache = cache_name(context)
    keys = Logflare.Cache.dispatch(cache, :keys_to_bust, [kw])
    Logflare.Cache.dispatch(cache, :delete_keys, [keys])
  end

  defp bust_key(context, pkey), do: bust_key(context, id: pkey)

  @spec cache_name(atom()) :: atom()
  def cache_name(context) do
    Module.concat(context, Cache)
  end
end
