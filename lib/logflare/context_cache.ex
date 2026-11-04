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
  keys other than `id:` override `c:keys_to_bust/1`.

  ## Memoization

  This module can also be used to cache heavy functions or db calls hidden behind a 3rd party
  library. See `Logflare.Auth.Cache` for an example, where the cache's own expiration is the only
  invalidation.
  """

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache.Tombstones

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

  @doc """
  Returns the tombstones to record, each with its time-to-live in ms (`nil` for the default), for
  a keyword of values of a busted change.

  Defaults to `Logflare.ContextCache.Tombstones.pkey_tombstones/1`.
  """
  @callback tombstones(keyword()) :: [{tombstone :: term(), ttl :: non_neg_integer() | nil}]

  defmacro __using__(opts) do
    impl = Keyword.get(opts, :impl, CachexOps)

    quote do
      use Logflare.Cache, unquote(opts)

      @behaviour Logflare.ContextCache

      @impl Logflare.ContextCache
      def keys_to_bust(kw), do: unquote(impl).keys_to_bust(__MODULE__, kw)

      @impl Logflare.ContextCache
      def delete_keys(keys), do: unquote(impl).delete_keys(__MODULE__, keys)

      @impl Logflare.ContextCache
      def fetch(key, getter), do: unquote(impl).fetch(__MODULE__, key, getter)

      @impl Logflare.ContextCache
      def update(key, value), do: unquote(impl).update(__MODULE__, key, value)

      @impl Logflare.ContextCache
      def tombstones(kw), do: unquote(Tombstones).pkey_tombstones(kw)

      defoverridable keys_to_bust: 1, delete_keys: 1, fetch: 2, update: 2, tombstones: 1
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
  or a keyword list with fields useful for busting. Both are passed to the context cache's
  `c:keys_to_bust/1`, a primary key as `[id: pkey]`, and the keys returned for all values of a
  context to a single `c:delete_keys/1` call.
  """
  @spec bust_keys(list()) :: {:ok, non_neg_integer()}
  def bust_keys(values) when is_list(values) do
    busted =
      values
      |> Enum.group_by(fn {context, _pkey_or_kw} -> context end, &to_kw/1)
      |> Enum.reduce(0, fn {context, kws}, acc ->
        cache = cache_name(context)

        {:ok, n} =
          kws |> Enum.flat_map(&cache.keys_to_bust/1) |> Enum.uniq() |> cache.delete_keys()

        acc + n
      end)

    {:ok, busted}
  end

  defp to_kw({_context, kw}) when is_list(kw), do: kw
  defp to_kw({_context, pkey}), do: [id: pkey]

  @spec cache_name(atom()) :: atom()
  def cache_name(context) do
    Module.concat(context, Cache)
  end
end
