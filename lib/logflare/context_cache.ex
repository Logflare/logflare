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
  """

  @type key() :: {fun :: atom(), args :: list()}

  @type actions() :: %{key() => any() | :bust}

  @type bust_ctx() :: integer() | keyword(integer())

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
  """
  @spec bust_keys(list()) :: :ok
  def bust_keys(values) when is_list(values) do
    for {context, primary_key} <- values do
      context_cache = cache_name(context)
      bust_key(context_cache, primary_key)
    end

    :ok
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
        value

      {:ok, {:cached, value}} ->
        value
    end
  end

  @spec refresh_keys([{module(), integer() | keyword(), {:full | :partial, actions()}}]) :: :ok
  def refresh_keys(values) when is_list(values) do
    Enum.each(values, fn {context, trigger, {tag, actions}} ->
      cache = cache_name(context)

      case {tag, trigger} do
        {:partial, trigger} when is_integer(trigger) ->
          # Remeber present keys to bring back only existing keys after busting
          present_keys = present_action_keys(cache, actions)
          bust_key(cache, trigger)
          apply_partial_actions(cache, actions, present_keys)

        _ ->
          apply_actions(cache, actions)
      end
    end)

    :ok
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
