defmodule Logflare.KeyValues.Cache do
  @moduledoc false

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache
  alias Logflare.KeyValues
  alias Logflare.Repo

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 10_000_000,
      ttl: to_timeout(day: 1),
      purge_interval: to_timeout(hour: 1),
      compressed: true,
      warmer: {KeyValues.CacheWarmer, interval: :timer.hours(1)}
    )
  end

  @spec count(integer()) :: non_neg_integer()
  def count(user_id) do
    cache_key = {:count, user_id}

    Cachex.fetch(__MODULE__, cache_key, fn _key ->
      {:commit, {:cached, Repo.apply_with_replica(KeyValues, :count_key_values, [user_id])}}
    end)
    |> case do
      {:commit, {:cached, v}} -> v
      {:ok, {:cached, v}} -> v
    end
  end

  @spec lookup(integer(), String.t()) :: map() | nil
  def lookup(user_id, key) do
    lookup(user_id, key, nil)
  end

  @spec lookup(integer(), String.t(), String.t() | nil) :: term() | nil
  def lookup(user_id, key, accessor_path) do
    key_args = [user_id, key, accessor_path]
    cache_key = {:lookup, key_args}

    Cachex.execute!(__MODULE__, fn worker ->
      Cachex.fetch(worker, cache_key, fn _key -> lookup_fallback(worker, key_args) end)
    end)
    |> case do
      {:commit, {:cached, v}} -> v
      {:ok, {:cached, v}} -> v
    end
  end

  defp lookup_fallback(worker, [user_id, key, accessor_path] = key_args) do
    case Cachex.get(worker, {:lookup, [user_id, key, nil]}) do
      {:ok, {:cached, full_value}} ->
        {:commit, {:cached, KeyValues.extract_value(full_value, accessor_path)}}

      {:ok, nil} ->
        {:commit, {:cached, Repo.apply_with_replica(KeyValues, :lookup, key_args)}}
    end
  end

  @impl ContextCache
  def keys_to_bust(kw) do
    user_id = Keyword.get(kw, :user_id)
    key = Keyword.get(kw, :key)

    entries = if user_id, do: [{:count, user_id}], else: []

    if user_id && key do
      lookup_keys = find_lookup_keys(user_id, key)
      lookup_keys ++ entries
    else
      entries
    end
  end

  defp find_lookup_keys(user_id, key) do
    {:ok, keys} = Cachex.keys(__MODULE__)

    Enum.filter(keys, fn
      {:lookup, [^user_id, ^key | _]} -> true
      _ -> false
    end)
  end
end
