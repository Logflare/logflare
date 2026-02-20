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
  """

  require Logger
  require Ex2ms

  @doc """
  Optional callback providing keys to bust based on a keyword defined in `Logflare.ContextCache.CacheBuster`
  """
  @callback keys_to_bust(keyword()) :: [{fun :: atom(), args :: list()}]

  @spec cache_name(atom()) :: atom()
  def cache_name(context) do
    Module.concat(context, Cache)
  end

  @spec apply_fun(module(), tuple() | atom(), list()) :: any()
  def apply_fun(context, {fun, _arity}, args), do: apply_fun(context, fun, args)

  def apply_fun(context, fun, args) when is_atom(fun) do
    cache = cache_name(context)
    cache_key = {fun, args}

    case Cachex.fetch(cache, cache_key, fn {fun, args} ->
           # Use a `:cached` tuple here otherwise when an fn returns nil Cachex will miss
           # the cache because it thinks ETS returned nil
           {:commit, {:cached, apply(context, fun, args)}}
         end) do
      {:commit, {:cached, value}} ->
        value

      {:ok, {:cached, value}} ->
        value
    end
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
  @spec bust_keys(list()) :: :ok
  def bust_keys(values) when is_list(values) do
    for {context, primary_key} <- values do
      context_cache = cache_name(context)
      bust_key(context_cache, primary_key)
    end

    :ok
  end

  defp bust_key(context_cache, kw) when is_list(kw) do
    Cachex.execute!(context_cache, fn cache ->
      context_cache.keys_to_bust(kw)
      |> delete_entries(cache)
    end)
  end

  defp bust_key(context_cache, pkey) do
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

  @spec refresh_keys(list()) :: {:ok, non_neg_integer()}
  def refresh_keys(values) when is_list(values) do
    for {context, primary_key} <- values do
      refresh_key(context, primary_key)
    end

    :ok
  end

  defp refresh_key(context, kw) when is_list(kw) do
    context_cache = cache_name(context)

    Cachex.execute!(context_cache, fn cache ->
      context_cache.keys_to_bust(kw)
      |> refresh_entries(context, cache)
    end)
  end

  defp refresh_key(context, pkey) do
    context_cache = cache_name(context)

    Cachex.execute!(context_cache, fn cache ->
      keys_with_id(cache, pkey)
      |> refresh_entries(context, cache)
    end)
  end

  defp refresh_entries(entries, context, cache) do
    Enum.each(entries, fn {fun, args} = key ->
      case Cachex.take(cache, key) do
        {:ok, nil} -> :ok
        {:ok, _} -> apply_fun(context, fun, args)
      end
    end)
  end

  defp keys_with_id(cache, id) do
    # match {_cached, %{id: ^pkey}}
    direct_filter =
      {:andalso, {:is_map, {:element, 2, :value}},
       {:==, {:map_get, :id, {:element, 2, :value}}, id}}

    # match {_cached, {:ok, %{id: ^pkey}}}
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
    |> Stream.map(&elem(&1, 0))
  end
end
