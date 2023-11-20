defmodule Logflare.ContextCache do
  @moduledoc """
  Read-through cache for hot database paths. This module functions as the entry point for
  contexts to have a cache of function calls.

  e.g. `Logflare.Users.Cache` functions are ran through `apply_fun/3` and restuls of those
  functions are returned to the caller and cached in the respective cache.

  The cache implementation of `Logflare.ContextCache` is a reverse index where values
  returned by functions are used as the cache key.

  We must keep a reverse index because function are called by their arguments. So in the
  `Logflare.Users.Cache` we can keep a key of the MFA and a value of the results.

  But when a record from the write-ahead log comes in the `CacheBuster` calls `bust_keys/1`
  and we must know what the key is in the `Logflare.Users.Cache` to bust.

  So we keep the value of the `Logflare.Users.Cache` as the key in the `Logflare.ContextCache`
  and the value of our `Logflare.ContextCache` key is the key for our `Logflare.Users.Cache`.
  """

  require Logger

  @cache __MODULE__

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)
    %{id: __MODULE__, start: {Cachex, :start_link, [@cache, [stats: stats]]}}
  end

  def apply_fun(context, {fun, arity}, args) do
    cache = cache_name(context)
    cache_key = {{fun, arity}, args}

    case Cachex.fetch(cache, cache_key, fn {{_fun, _arity}, args} ->
           # Use a `:cached` tuple here otherwise when an fn returns nil Cachex will miss the cache because it thinks ETS returned nil
           {:commit, {:cached, apply(context, fun, args)}}
         end) do
      {:commit, {:cached, value}} ->
        index_keys(context, cache_key, value)
        value

      {:ok, {:cached, value}} ->
        # already cached don't re-index
        value
    end
  end

  @doc """
  This function is called from the CacheBuster process when a new record comes in from the Postgres
  write-ahead log. The WAL contains records. From those records the CacheBuster picks out
  primary keys.

  The records ARE the keys in the reverse cache (the ContextCache).

  We must:
   - Find the key by the record primary key
   - Delete the reverse cache entry
   - Delete the cache entry for that context cache e.g. `Logflare.Users.Cache`
  """

  def bust_keys(values) do
    {:ok, keys} = Cachex.keys(@cache)

    total =
      Enum.count(keys, fn {token, cache_key} = key ->
        with true <- token in values,
             {context, _} = token,
             context_cache = cache_name(context),
             {:ok, true} <- Cachex.del(context_cache, cache_key) do
          Cachex.del(@cache, key)
        end

        true
      end)

    :telemetry.execute(
      [:logflare, :context_cache, :busted],
      %{count: total},
      %{}
    )

    {:ok, :busted}
  end

  def bust_keys(context, id), do: bust_keys([{context, id}])

  @spec cache_name(atom()) :: atom()
  def cache_name(context) do
    Module.concat(context, Cache)
  end

  defp index_keys(context, cache_key, value) do
    keys_key = {{context, select_key(value)}, cache_key}

    Cachex.put(@cache, keys_key, cache_key)

    {:ok, :indexed}
  end

  defp select_key(value) do
    case value do
      %Logflare.Source{} ->
        value.id

      %Logflare.Billing.BillingAccount{} ->
        value.id

      %Logflare.User{} ->
        value.id

      %Logflare.Billing.Plan{} ->
        value.id

      true ->
        # Logger.warning("Cached unknown value from context.", error_string: inspect(value))
        "true"

      nil ->
        # Logger.warning("Cached unknown value from context.", error_string: inspect(value))
        :not_found

      _value ->
        # Logger.warning("Unhandled cache key for value.", error_string: inspect(value))
        :uknown
    end
  end
end
