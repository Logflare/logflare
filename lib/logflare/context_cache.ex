defmodule Logflare.ContextCache do
  @moduledoc """
    Read through cache for hot database paths.
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
