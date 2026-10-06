defmodule Logflare.KeyValues.Cache do
  @moduledoc false

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache
  alias Logflare.ContextCache.Tombstones
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
    cache_key = {:lookup, [user_id, key, accessor_path]}

    Cachex.fetch(__MODULE__, cache_key, fn _key ->
      {:commit,
       {:cached, Repo.apply_with_replica(KeyValues, :lookup, [user_id, key, accessor_path])}}
    end)
    |> case do
      {:commit, {:cached, v}} -> v
      {:ok, {:cached, v}} -> v
    end
  end

  @impl ContextCache
  def bust_by(kw) do
    CachexOps.delete_keys(__MODULE__, bust_entries(kw))
  end

  @impl ContextCache
  def tombstones(kw) when is_list(kw) do
    case {Keyword.get(kw, :user_id), Keyword.get(kw, :key)} do
      {nil, _key} -> []
      {user_id, nil} -> [{:count, user_id}]
      {user_id, key} -> [{:count, user_id}, {:key, user_id, key}]
    end
  end

  def tombstones(_pkey), do: []

  @impl ContextCache
  def stale_entry?({:lookup, [user_id, key | _accessor]}, _value),
    do: Tombstones.Cache.tombstoned?(__MODULE__, {:key, user_id, key})

  def stale_entry?({:count, user_id}, _value),
    do: Tombstones.Cache.tombstoned?(__MODULE__, {:count, user_id})

  def stale_entry?(_key, _value), do: true

  defp bust_entries(kw) do
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
