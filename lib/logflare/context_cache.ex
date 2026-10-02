defmodule Logflare.ContextCache do
  @moduledoc """
  Read-through cache for hot database paths and/or functions. This module functions as the entry point for
  contexts to have a cache of function calls.

  e.g. `Logflare.Users.Cache` functions go through `apply_fun/3` and results of those
  functions are returned to the caller and cached in the respective cache.

  ## Implementation

  `use Logflare.ContextCache` makes the module a `Logflare.Cache` and injects overridable defaults
  for `c:fetch/2`, `c:update/2` and `c:bust_by/1`. They delegate to a `Logflare.ContextCache.Ops`
  module, `Logflare.Cache.CachexOps` unless `impl: module` is given.

  ## Busting

  `bust_keys/1` busts entries by primary key or by a keyword of fields. Caches that need busting
  keys other than `id:` override `c:bust_by/1`.

  ## Memoization

  This module can also be used to cache heavy functions or db calls hidden behind a 3rd party
  library. See `Logflare.Auth.Cache` for an example, where the cache's own expiration is the only
  invalidation.
  """

  alias Logflare.Cache.CachexOps

  @doc """
  Busts cache entries by a keyword of values, returning the number of entries busted.
  """
  @callback bust_by(keyword()) :: {:ok, non_neg_integer()} | {:error, term()}

  @doc """
  Returns the cached value for `key`, calling `getter` and caching its result on a miss.
  """
  @callback fetch(key :: term(), getter :: (-> term())) :: term()

  @doc """
  Replaces the value cached for `key`. Does nothing when `key` is not cached.
  """
  @callback update(key :: term(), value :: term()) :: :ok

  defmacro __using__(opts) do
    impl = Keyword.get(opts, :impl, CachexOps)

    quote do
      use Logflare.Cache, unquote(opts)

      @behaviour Logflare.ContextCache

      @impl Logflare.ContextCache
      def bust_by(kw), do: unquote(impl).bust_by(__MODULE__, kw)

      @impl Logflare.ContextCache
      def fetch(key, getter), do: unquote(impl).fetch(__MODULE__, key, getter)

      @impl Logflare.ContextCache
      def update(key, value), do: unquote(impl).update(__MODULE__, key, value)

      defoverridable bust_by: 1, fetch: 2, update: 2
    end
  end

  @spec apply_fun(module(), tuple() | atom(), list()) :: any()
  def apply_fun(context, {fun, _arity}, args), do: apply_fun(context, fun, args)

  def apply_fun(context, fun, args) when is_atom(fun) do
    cache_name(context).fetch({fun, args}, fn ->
      Logflare.Repo.apply_with_replica(context, fun, args)
    end)
  end

  @doc """
  Updates cache entry to the given value
  """
  @spec update(module(), atom(), list(), term()) :: :ok
  def update(context, fun, args, value) when is_atom(fun) do
    cache_name(context).update({fun, args}, value)
  end

  @doc """
  Busts cache entries based on context-primary-key pairs.

  It is intended for following a WAL for cache busting. When a new record comes in from the WAL,
  the CacheBuster process calls this function with either the primary keys extracted from those records
  or a keyword list with fields useful for busting. Both are passed to the context cache's `c:bust_by/1`,
  a primary key as `[id: pkey]`.
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

  defp bust_key(context, kw) when is_list(kw), do: cache_name(context).bust_by(kw)
  defp bust_key(context, pkey), do: cache_name(context).bust_by(id: pkey)

  @spec cache_name(atom()) :: atom()
  def cache_name(context) do
    Module.concat(context, Cache)
  end
end
