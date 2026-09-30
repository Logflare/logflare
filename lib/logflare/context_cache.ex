defmodule Logflare.ContextCache do
  @moduledoc """
  Read-through cache for hot database paths and/or functions. This module functions as the entry point for
  contexts to have a cache of function calls.

  e.g. `Logflare.Users.Cache` functions go through `apply_fun/3` and results of those
  functions are returned to the caller and cached in the respective cache.

  ## Implementation

  `use Logflare.ContextCache` makes the module a `Logflare.Cache` and injects overridable defaults
  for every callback. They delegate to a `Logflare.ContextCache.Ops` module,
  `Logflare.Cache.CachexOps` unless `impl: module` is given.

  ## Entries

  `c:entries/0` and `c:put_entries/1` read and write entries as `t:entry/0`, independent of the
  storage backend, so a cache can be copied between nodes running different backends.

  ## Refresh-ahead

  With `refresh_ahead: true`, `c:fetch/2` also hands the key to `Logflare.ContextCache.RefreshAhead`,
  which reloads entries close to expiry in the background, so reads don't block on the getter
  when a hot entry expires. It relies on `c:expiry/1`, `c:cached?/1` and `c:put_entries/1` only.

  ## Busting

  `bust_keys/1` busts entries by primary key or by a keyword of fields. Caches that need busting
  keys other than `id:` override `c:bust_by/1`, and `c:tombstones/1` with `c:stale_entry?/2` when
  entries received from other nodes should be checked against those keys.

  ## Memoization

  This module can also be used to cache heavy functions or db calls hidden behind a 3rd party
  library. See `Logflare.Auth.Cache` for an example, where the cache's own expiration is the only
  invalidation.
  """

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache.Gossip
  alias Logflare.ContextCache.RefreshAhead

  @typedoc """
  A cached value under `key`, with its remaining time-to-live in ms, `nil` when it does not expire.
  """
  @type entry() :: {key :: term(), value :: term(), ttl :: pos_integer() | nil}

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

  @doc """
  Streams the unexpired entries. The stream must be consumed in the process that created it.
  """
  @callback entries() :: Enumerable.t(entry())

  @doc """
  Writes `entries`, replacing values cached under the same keys.
  """
  @callback put_entries([entry()]) :: :ok

  @callback cached?(key :: term()) :: boolean()

  @doc """
  Remaining and total time-to-live in ms of the entry cached under `key`, `nil` when it is not
  cached or does not expire.
  """
  @callback expiry(key :: term()) ::
              {remaining :: non_neg_integer(), total :: pos_integer()} | nil

  @callback size() :: non_neg_integer()

  @doc """
  Tombstones to record when a record of this cache changes, given the primary key or keyword passed
  to `bust_keys/1`. See `Logflare.ContextCache.Gossip`.
  """
  @callback tombstones(pkey_or_kw :: term()) :: [term()]

  @doc """
  Whether `value` cached under `key` on another node must not be cached on this one, because its
  record changed recently or there is no tombstone to check it against.
  """
  @callback stale_entry?(key :: term(), value :: term()) :: boolean()

  defmacro __using__(opts) do
    impl = Keyword.get(opts, :impl, CachexOps)
    refresh_ahead? = Keyword.get(opts, :refresh_ahead, false)

    quote do
      use Logflare.Cache, unquote(opts)

      @behaviour Logflare.ContextCache

      @impl Logflare.ContextCache
      def bust_by(kw), do: unquote(impl).bust_by(__MODULE__, kw)

      @impl Logflare.ContextCache
      def fetch(key, getter) do
        value = unquote(impl).fetch(__MODULE__, key, getter)

        if unquote(refresh_ahead?),
          do: unquote(RefreshAhead).maybe_refresh(__MODULE__, key, getter)

        value
      end

      @impl Logflare.ContextCache
      def update(key, value), do: unquote(impl).update(__MODULE__, key, value)

      @impl Logflare.ContextCache
      def entries, do: unquote(impl).entries(__MODULE__)

      @impl Logflare.ContextCache
      def put_entries(entries), do: unquote(impl).put_entries(__MODULE__, entries)

      @impl Logflare.ContextCache
      def cached?(key), do: unquote(impl).cached?(__MODULE__, key)

      @impl Logflare.ContextCache
      def expiry(key), do: unquote(impl).expiry(__MODULE__, key)

      @impl Logflare.ContextCache
      def size, do: unquote(impl).size(__MODULE__)

      @impl Logflare.ContextCache
      def tombstones(pkey_or_kw), do: unquote(Gossip).pkey_tombstones(pkey_or_kw)

      @impl Logflare.ContextCache
      def stale_entry?(_key, value), do: unquote(Gossip).stale_value?(__MODULE__, value)

      defoverridable bust_by: 1,
                     fetch: 2,
                     update: 2,
                     entries: 0,
                     put_entries: 1,
                     cached?: 1,
                     expiry: 1,
                     size: 0,
                     tombstones: 1,
                     stale_entry?: 2
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
