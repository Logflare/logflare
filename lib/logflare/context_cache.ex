defmodule Logflare.ContextCache do
  @moduledoc """
    Read through cache for hot database paths.

    Stats are reported to Logflare via Logflare.SystemMetrics.Cachex.Poller.

    TODO
     - Limit this cache like the others
     - Cachex hook when other cache keys are evicted the keys key here gets deletedf
     - Cachex hook on this one when it's limited, where if it gets evicted it invalidates the others
  """

  require Logger

  @cache __MODULE__

  def child_spec(_) do
    %{id: __MODULE__, start: {Cachex, :start_link, [@cache, [stats: false]]}}
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
        index_keys(context, cache_key, value)
        value
    end
  end

  def bust_keys(context, id) do
    context_cache = cache_name(context)

    {:ok, keys} = Cachex.keys(@cache)

    Stream.each(keys, fn
      {{^context, ^id}, cache_key} = key ->
        case Cachex.del(context_cache, cache_key) do
          {:ok, true} ->
            Cachex.del(@cache, key)

          {:ok, nil} ->
            :noop
        end

      _key ->
        :noop
    end)
    |> Stream.run()

    {:ok, :busted}
  end

  defp index_keys(context, cache_key, value) do
    keys_key = {{context, select_key(value)}, cache_key}

    {:ok, key} = Cachex.get(@cache, keys_key)

    if is_nil(key), do: Cachex.put(@cache, keys_key, cache_key)

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
        "true"

      nil ->
        # Logger.warn("Cached unknown value from context.", error_string: inspect(value))
        :not_found

      _value ->
        # Logger.warn("Unhandled cache key for value.", error_string: inspect(value))
        :uknown
    end
  end

  defp cache_name(context) do
    Module.concat(context, Cache)
  end
end
