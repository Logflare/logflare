defmodule Logflare.ContextCache do
  @moduledoc false

  require Logger

  @cache __MODULE__

  def child_spec(_) do
    %{id: __MODULE__, start: {Cachex, :start_link, [@cache, []]}}
  end

  def apply_fun(context, {fun, arity}, args) do
    cache = cache_name(context)
    cache_key = {{fun, arity}, args}

    case Cachex.fetch(cache, cache_key, fn {{_fun, _arity}, args} ->
           # If this `apply` returns nil cachex thinks it didn't find anything and actually runs the function vs returning the cached nil
           {:commit, apply(context, fun, args)}
         end) do
      {:commit, value} ->
        index_keys(context, cache_key, value)
        value

      {:ok, value} ->
        index_keys(context, cache_key, value)
        value
    end
  end

  def bust_keys(context, id) when is_integer(id) do
    context_cache = cache_name(context)
    key = {context, id}

    {:ok, keys} = Cachex.get(@cache, key)

    if keys do
      # Logger.info("Cache busted for `#{context}`")

      # Should probably also update this to delete or update our keys index but we'll keep them all here to avoid race conditions for now
      for(k <- keys, do: Cachex.del(context_cache, k))
    end

    {:ok, :busted}
  end

  defp index_keys(context, cache_key, value) do
    keys_key = {context, select_key(value)}

    {:ok, keys} = Cachex.get(@cache, keys_key)

    cond do
      is_nil(keys) ->
        Cachex.put(@cache, keys_key, [cache_key])

      Enum.any?(keys, &match?(&1, cache_key)) ->
        :noop

      true ->
        Cachex.put(@cache, keys_key, [cache_key | keys])
    end

    {:ok, :indexed}
  end

  defp select_key(value) do
    case value do
      %Logflare.Source{} ->
        value.id

      %Logflare.BillingAccounts.BillingAccount{} ->
        value.id

      %Logflare.User{} ->
        value.id

      %Logflare.Plans.Plan{} ->
        value.id

      true ->
        # Logger.warn("Cached unknown value from context.", error_string: inspect(value))
        :unknown

      nil ->
        # Logger.warn("Cached unknown value from context.", error_string: inspect(value))
        :unknown

      _value ->
        # Logger.warn("Unhandled cache key for value.", error_string: inspect(value))
        :uknown
    end
  end

  defp cache_name(context) do
    Module.concat(context, Cache)
  end
end
