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

  If customization of busting is needed, cache module may implement `c:bust_actions/1` callback.

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

  alias Logflare.ContextCache.Gossip

  @type key() :: {fun :: atom(), args :: list()}

  @type actions() :: %{key() => any() | :bust}

  @type bust_ctx() :: integer() | keyword(integer())

  @type plan() :: {:full | :partial, actions()}

  @type update_item() :: {context :: module(), bust_ctx(), plan()}

  @doc """
  Returns a tagged tuple of cache actions to perform when a DB change is detected.

  The tag indicates coverage:
  - `:full` — the map covers all cached functions in this module. Only the map is applied.
  - `:partial` — the map covers some cached functions. The map is applied, and for integer
    triggers an ETS scan also runs to bust any remaining entries containing the trigger ID.
    This is a temporary state during migration; the goal is to reach `:full` coverage.

  Each key in the map is a cache key (`{fun, args}` tuple). The value is either a fresh
  value to store under that key, or `:bust` to delete the entry.

  Used by `CacheBuster` to pre-fetch and broadcast cache updates cluster-wide.
  """
  @callback bust_actions(action, bust_ctx()) :: {:full | :partial, actions()}
            when action: :update | :delete

  @optional_callbacks [bust_actions: 2]

  @spec cache_name(atom()) :: atom()
  def cache_name(context) do
    Module.concat(context, Cache)
  end

  @spec apply_fun(module(), tuple() | atom(), list()) :: any()
  def apply_fun(context, {fun, _arity}, args), do: apply_fun(context, fun, args)

  def apply_fun(context, fun, args) when is_atom(fun) do
    cache = cache_name(context)
    cache_key = {fun, args}

    fetch(cache, cache_key, fn ->
      Logflare.Repo.apply_with_random_repo(context, fun, args)
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

  @doc """
  Applies a list of cache updates produced by `CacheBuster` (or by direct callers
  performing manual invalidation).

  Each item is `{context, trigger, plan}` where `plan` is one of:

  - `{:full, actions}` — the action map covers every cached function; apply
    in-place (`Cachex.update`) to existing keys and `Cachex.del` for `:bust` values.
  - `{:partial, actions}` — like `:full` but combined with an ETS scan over the
    integer trigger so unlisted variants get evicted. Re-inserts only the keys
    that were present before the scan to avoid warming the cache with unrequested data.
    With a non-integer trigger no scan runs (the keyword shape carries enough
    information for the action map to be authoritative).

  Pass `{:partial, %{}}` to get a pure scan-and-delete by integer pkey;
  `bust_keys/1` is the convenience wrapper for that case.
  """
  @spec refresh_keys([update_item()]) :: :ok
  def refresh_keys(items) when is_list(items) do
    Enum.each(items, &apply_update/1)
    :ok
  end

  @doc """
  Convenience wrapper around `refresh_keys/1` for callers that just want to
  evict all cache entries referencing a primary key.
  """
  @spec bust_keys([{module(), integer()}]) :: :ok
  def bust_keys(values) when is_list(values) do
    values
    |> Enum.map(fn {context, pkey} -> {context, pkey, {:partial, %{}}} end)
    |> refresh_keys()
  end

  defp apply_update({context, _trigger, {:full, actions}}) do
    apply_actions(cache_name(context), actions)
  end

  defp apply_update({context, trigger, {:partial, actions}}) when is_integer(trigger) do
    cache = cache_name(context)
    present_keys = present_action_keys(cache, actions)
    bust_key(cache, trigger)
    apply_partial_actions(cache, actions, present_keys)
  end

  defp apply_update({context, _trigger, {:partial, actions}}) do
    apply_actions(cache_name(context), actions)
  end

  defp bust_key(context_cache, pkey) when is_integer(pkey) do
    Cachex.execute!(context_cache, fn cache ->
      keys_with_id(cache, pkey)
      |> delete_entries(cache)
    end)
  end

  defp delete_entries(entries, cache) do
    Enum.each(entries, fn key ->
      Cachex.del(cache, key)
    end)
  end

  defp present_action_keys(cache, actions) do
    Cachex.execute!(cache, fn worker ->
      for {cache_key, value} <- actions,
          value != :bust,
          {:ok, true} == Cachex.exists?(worker, cache_key),
          into: MapSet.new() do
        cache_key
      end
    end)
  end

  defp apply_actions(cache, actions) do
    Enum.each(actions, fn
      {cache_key, :bust} -> Cachex.del(cache, cache_key)
      {cache_key, value} -> Cachex.update(cache, cache_key, {:cached, value})
    end)
  end

  defp apply_partial_actions(cache, actions, present_keys) do
    Enum.each(actions, fn
      {cache_key, :bust} ->
        Cachex.del(cache, cache_key)

      {cache_key, value} ->
        if MapSet.member?(present_keys, cache_key),
          do: Cachex.put(cache, cache_key, {:cached, value})
    end)
  end

  defp keys_with_id(cache, id) do
    keys_with_id_stream(cache, id)
    |> Stream.map(&elem(&1, 0))
  end

  defp keys_with_id_stream(cache, id) do
    # match {_cached, %{id: ^pkey}}
    direct_filter =
      {:andalso, {:is_map, {:element, 2, :value}},
       {:==, {:map_get, :id, {:element, 2, :value}}, id}}

    # match {_cached, {:ok, %{id: ^pkey}}} and larger ok-tuples
    tuple_filter =
      {:andalso, {:is_tuple, {:element, 2, :value}},
       {:andalso, {:==, {:element, 1, {:element, 2, :value}}, :ok},
        {:andalso, {:is_map, {:element, 2, {:element, 2, :value}}},
         {:==, {:map_get, :id, {:element, 2, {:element, 2, :value}}}, id}}}}

    element_filter =
      {:orelse, direct_filter, tuple_filter}

    list_filter = {:is_list, {:element, 2, :value}}
    filter = {:orelse, list_filter, element_filter}
    query = Cachex.Query.build(where: filter, output: {:key, :value})

    cache
    |> Cachex.stream!(query)
    |> Stream.filter(fn
      {_k, {:cached, v}} when is_list(v) ->
        Enum.any?(v, &(&1.id == id))

      {_k, _v} ->
        true
    end)
  end
end
