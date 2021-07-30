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

    case Cachex.fetch(cache, cache_key, fn {_type, args} ->
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
    key = {context, id}

    {:ok, keys} = Cachex.get(@cache, key)

    if keys do
      # Logger.info("Cache busted for `#{context}`")

      # Should probably also update this to delete or update our keys index but we'll keep them all here to avoid race conditions for now
      for k <- keys, do: Cachex.del(cache, k)
    end

    {:ok, :busted}
  end

  defp index_keys(context, cache_key, value) do
    keys_key = {context, select_key(value)}

    Cachex.get_and_update(@cache, keys_key, fn
      nil ->
        {:commit, [cache_key]}

      keys ->
        {:commit, [cache_key | keys] |> Enum.uniq()}
    end)
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
