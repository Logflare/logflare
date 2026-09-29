defmodule Logflare.ContextCache do
  @moduledoc """
  Read-through cache for hot database paths and/or functions. This module functions as the entry point for
  contexts to have a cache of function calls.

  e.g. `Logflare.Users.Cache` functions go through `apply_fun/3` and results of those
  functions are returned to the caller and cached in the respective cache.

  ## Cache Implementation

  The cache implementation directly queries the relevant context cache to be busted and performs
  primary key checking within the matchspec. This approach queries across a narrower set of records,
  providing better performance compared to a reverse index approach.

  `use Logflare.ContextCache` makes the module a `Logflare.Cache` and injects a default `c:bust_by/1`
  that busts by `id:` (see `bust_by/2`). Caches that need other busting keys override it.

  ## List Busting

  The cache supports busting records within lists. If a struct in a non-empty list contains
  the :id field, the record will get busted when that ID is encountered in the write-ahead log.

  ## Memoization

  This module can also be used to cache heavy functions or db calls hidden behind a 3rd party
  library. See `Logflare.Auth.Cache` for an example. In this example, the `expiration` set in that
  Cachex child_spec is handling the cache expiration.

  In the case functions don't return a response with a primary key, or something else we can
  bust the cache on, it will get reverse indexed with `select_key/1` as `:unknown`.

  ## Gossip

  Cache misses are optionally multicast to peer nodes via `:erpc` to warm the cluster.
  To prevent race conditions, WAL invalidations write short-lived tombstones that
  filter out stale incoming messages.
  """

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache.Gossip

  @doc """
  Busts cache entries by a keyword of values, returning the number of entries busted.
  """
  @callback bust_by(keyword()) :: {:ok, non_neg_integer()} | {:error, term()}

  defmacro __using__(opts) do
    quote do
      use Logflare.Cache, unquote(opts)

      @behaviour Logflare.ContextCache

      @impl Logflare.ContextCache
      def bust_by(kw), do: Logflare.ContextCache.bust_by(__MODULE__, kw)

      defoverridable bust_by: 1
    end
  end

  @spec apply_fun(module(), tuple() | atom(), list()) :: any()
  def apply_fun(context, {fun, _arity}, args), do: apply_fun(context, fun, args)

  def apply_fun(context, fun, args) when is_atom(fun) do
    cache = cache_name(context)
    cache_key = {fun, args}

    fetch(cache, cache_key, fn ->
      Logflare.Repo.apply_with_replica(context, fun, args)
    end)
  end

  @doc """
  Updates cache entry to the given value
  """
  def update(context, fun, args, value) when is_atom(fun) do
    cache = cache_name(context)
    cache_key = {fun, args}

    Cachex.update(cache, cache_key, {:cached, value})
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

  @doc """
  Default `c:bust_by/1`: with `[id: pkey]`, busts every entry whose cached value is a map with that `:id`,
  an `{:ok, map}` with that `:id`, or a list containing such a map. Raises `ArgumentError` for any other keyword.
  """
  @spec bust_by(Cachex.t(), keyword()) :: {:ok, non_neg_integer()}
  def bust_by(cache, id: pkey) do
    filter =
      {
        # use orelse to prevent 2nd condition failing as value is not a map
        :orelse,
        {
          :orelse,
          # handle lists
          {:is_list, {:element, 2, :value}},
          # handle :ok tuples when struct with id is in 2nd element pos.
          {:andalso, {:is_tuple, {:element, 2, :value}},
           {:andalso, {:==, {:element, 1, {:element, 2, :value}}, :ok},
            {:andalso, {:is_map, {:element, 2, {:element, 2, :value}}},
             {:==, {:map_get, :id, {:element, 2, {:element, 2, :value}}}, pkey}}}}
        },
        # handle single maps
        {:andalso, {:is_map, {:element, 2, :value}},
         {:==, {:map_get, :id, {:element, 2, :value}}, pkey}}
      }

    query = Cachex.Query.build(where: filter, output: {:key, :value})

    keys =
      cache
      |> Cachex.stream!(query)
      |> Stream.filter(fn
        {_k, {:cached, v}} when is_list(v) -> Enum.any?(v, &(&1.id == pkey))
        {_k, _v} -> true
      end)
      |> Stream.map(fn {k, _v} -> k end)

    CachexOps.delete_keys(cache, keys)
  end

  def bust_by(cache, kw) do
    raise ArgumentError, "#{inspect(cache)} does not support busting by #{inspect(kw)}"
  end

  @spec cache_name(atom()) :: atom()
  def cache_name(context) do
    Module.concat(context, Cache)
  end

  @doc """
  Low level API for fetching from cache. Allows wrapping calls with
  `Cachex.execute/2` and accessing arbitrary key or calling any getter function.
  """
  @spec fetch(Cachex.t(), {atom(), list()}, fun()) :: term()
  def fetch(cache, cache_key, getter_fn) do
    case Cachex.fetch(cache, cache_key, fn _cache_key ->
           # Use a `:cached` tuple here otherwise when an fn returns nil Cachex will miss
           # the cache because it thinks ETS returned nil
           {:commit, {:cached, getter_fn.()}}
         end) do
      {:commit, {:cached, value}} ->
        Gossip.multicast(cache, cache_key, value)
        value

      {:ok, {:cached, value}} ->
        value
    end
  end
end
