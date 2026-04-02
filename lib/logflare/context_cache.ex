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

  If customization of busting is needed, cache module may implement `c:bust_by/1` callback expecting
  a keyword list instead of primary key for entry.

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

  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache.Tombstones

  @doc """
  Optional callback implementing custom cache key busting by a keyword of values
  """
  @callback bust_by(keyword()) :: {:ok, non_neg_integer()} | {:error, term()}

  @spec apply_fun(module(), tuple() | atom(), list()) :: any()
  def apply_fun(context, {fun, _arity}, args), do: apply_fun(context, fun, args)

  def apply_fun(context, fun, args) when is_atom(fun) do
    cache = cache_name(context)
    cache_key = {fun, args}

    fetch(cache, cache_key, fn -> apply(context, fun, args) end)
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
  or a keyword list with fields useful for busting.

  For primary key, the function then:

  1. Queries the relevant context cache using a matchspec to find entries to bust
  2. Handles both single records and lists of records containing matching IDs
  3. Deletes matching cache entries

  For keywords, it expects the cache to handle busting by implementing `c:bust_by/1`
  """
  @spec bust_keys(list()) :: {:ok, non_neg_integer()}
  def bust_keys(values) when is_list(values) do
    busted =
      for {context, primary_key} <- values, reduce: 0 do
        acc ->
          {:ok, n} = bust_key({context, primary_key})
          acc + n
      end

    {:ok, busted}
  end

  defp bust_key({context, kw}) when is_list(kw) do
    context_cache = cache_name(context)
    context_cache.bust_by(kw)
  end

  defp bust_key({context, pkey}) do
    context_cache = cache_name(context)

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

    query =
      Cachex.Query.build(where: filter, output: {:key, :value})

    context_cache
    |> Cachex.stream!(query)
    |> delete_matching_entries(context_cache, pkey)
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
        maybe_gossip(cache, cache_key, value)
        value

      {:ok, {:cached, value}} ->
        value
    end
  end

  defp delete_matching_entries(entries, context_cache, pkey) do
    to_delete =
      entries
      |> Stream.filter(fn
        {_k, {:cached, v}} when is_list(v) ->
          Enum.any?(v, &(&1.id == pkey))

        {_k, _v} ->
          true
      end)

    Cachex.execute(context_cache, fn worker ->
      Enum.reduce(to_delete, 0, fn {k, _v}, acc ->
        Cachex.del(worker, k)
        acc + 1
      end)
    end)
  end

  require Cachex.Spec

  # Negative lookups (`nil` or `[]`) are not gossiped. If Node A caches `nil`,
  # and the record is immediately created, a delayed `nil` cast to Node B
  # would cause phantom "not found" lookups while the record actually exists in the database.
  defp maybe_gossip(_cache, _key, nil), do: :ok
  defp maybe_gossip(_cache, _key, []), do: :ok

  # Explicitly ignore high-volume/ephemeral caches
  defp maybe_gossip(Logflare.Logs.LogEvents.Cache, _key, _value), do: :ok

  defp maybe_gossip(Cachex.Spec.cache(name: cache), key, value) do
    maybe_gossip(cache, key, value)
  end

  defp maybe_gossip(cache, key, value) when is_atom(cache) do
    meta = %{cache: cache}

    :telemetry.span([:logflare, :context_cache_gossip, :multicast], meta, fn ->
      %{enabled: enabled, ratio: ratio, max_nodes: max_nodes} =
        config = Application.fetch_env!(:logflare, :context_cache_gossip)

      if enabled do
        peers = ClusterUtils.peer_list_partial(ratio, max_nodes)
        :erpc.multicast(peers, __MODULE__, :receive_gossip, [cache, key, value])
      end

      {:ok, Map.merge(config, meta)}
    end)
  end

  @doc false
  def receive_gossip(cache, key, value) do
    meta = %{cache: cache, key: key}

    :telemetry.span([:logflare, :context_cache_gossip, :receive], meta, fn ->
      action =
        cond do
          # refresh if the node already has this cache key
          Cachex.exists?(cache, key) == {:ok, true} ->
            Cachex.refresh(cache, key)
            :refreshed

          # do nothing if the WAL recently busted this specific record
          tombstoned?(cache, value) ->
            :dropped_stale

          true ->
            Cachex.put(cache, key, {:cached, value})
            :cached
        end

      {:ok, Map.put(meta, :action, action)}
    end)
  end

  @doc false
  def record_tombstones(context_pkeys) when is_list(context_pkeys) do
    # Writes a short-lived marker for a primary key indicating it was recently updated or deleted.
    # Incoming cache broadcasts check this tombstone cache to determine if their payload could be stale.
    Enum.each(context_pkeys, fn context_pkey ->
      if tombstone = to_tombstone(context_pkey) do
        Tombstones.Cache.put_tombstone(tombstone)
      end
    end)
  end

  defp to_tombstone(context_pkey) do
    case context_pkey do
      {context, id} when is_integer(id) or is_binary(id) ->
        {cache_name(context), id}

      {context, info} when is_list(info) ->
        if id = Keyword.get(info, :id) do
          {cache_name(context), id}
        end

      {context, %{id: id}} ->
        {cache_name(context), id}

      _other ->
        nil
    end
  end

  defp tombstoned?(cache, value) do
    pkeys = extract_pkeys(value)
    Enum.any?(pkeys, fn pkey -> Tombstones.Cache.tombstoned?(cache, pkey) end)
  end

  defp extract_pkeys(values) when is_list(values) do
    Enum.flat_map(values, &extract_pkeys/1)
  end

  defp extract_pkeys({:ok, value}), do: extract_pkeys(value)
  defp extract_pkeys(%{id: id}), do: [id]
  defp extract_pkeys(_), do: []
end
