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
  @spec apply_fun(atom(), tuple() | atom(), [list()]) :: any()
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
  Busts cache entries based on context-primary-key pairs.

  It is intended for following a WAL for cache busting.When a new record comes in from the WAL, the CacheBuster process calls this function
  with the primary keys extracted from those records. The function then:

  1. Queries the relevant context cache using a matchspec to find entries to bust
  2. Handles both single records and lists of records containing matching IDs
  3. Deletes matching cache entries
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
    |> Enum.reduce(0, fn
      {k, {:cached, v}}, acc when is_list(v) ->
        if Enum.any?(v, fn %{id: id} -> id == pkey end) do
          Cachex.del(context_cache, k)
          acc + 1
        else
          acc
        end

      {k, _v}, acc ->
        Cachex.del(context_cache, k)
        acc + 1
    end)
    |> then(&{:ok, &1})
  end

  @spec cache_name(atom()) :: atom()
  def cache_name(context) do
    Module.concat(context, Cache)
  end
end
