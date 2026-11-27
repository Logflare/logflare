defmodule Logflare.KeyValues.CacheWarmer do
  @moduledoc false

  use Cachex.Warmer

  alias Logflare.KeyValues.Cache
  alias Logflare.KeyValues.KeyFilter
  alias Logflare.KeyValues.KeyValue
  alias Logflare.KeyValues.KeyValueUsage
  alias Logflare.Repo

  require Logger
  import Ecto.Query

  @pt_key {__MODULE__, :initialized}
  @pt_warmed_at {__MODULE__, :warmed_at}
  @warm_limit Application.compile_env!(:logflare, [__MODULE__, :warm_limit])
  @full_warm_interval to_timeout(hour: 12)
  @filter_page_size 10_000

  @impl true
  def execute(_state) do
    if initialized?() and not full_warm_due?() do
      Repo.apply_with_replica(__MODULE__, :warm_recent, [])
    else
      try do
        {:ok, :ok} = Repo.apply_with_replica(__MODULE__, :warm_all, [])
        sweep()
        :persistent_term.put(@pt_warmed_at, DateTime.utc_now())
        :persistent_term.put(@pt_key, true)
      rescue
        e ->
          Logger.error("Error performing full KeyValues.Cache warming: #{inspect(e)}")
      end
    end

    :ignore
  end

  @doc """
  Rebuilds the `KeyFilter` from every key, then warms values with `warm_top_n/1`.
  """
  @spec warm_all() :: {:ok, :ok} | {:error, term()}
  def warm_all do
    {:ok, _count} = rebuild_filter()
    warm_top_n()
  end

  @doc """
  Rebuilds the `KeyFilter` from every key.
  """
  @spec rebuild_filter() :: {:ok, non_neg_integer()}
  def rebuild_filter do
    count =
      KeyValue
      |> Repo.aggregate(:count)
      |> KeyFilter.rebuild(&add_keys_after(&1, 0, 0))

    {:ok, count}
  end

  defp add_keys_after(filter, last_id, count) do
    rows =
      KeyValue
      |> where([kv], kv.id > ^last_id)
      |> order_by(:id)
      |> limit(@filter_page_size)
      |> select([kv], {kv.id, kv.user_id, kv.key})
      |> Repo.all()

    KeyFilter.add_all(filter, for({_id, user_id, key} <- rows, do: {user_id, key}))

    case List.last(rows) do
      nil -> count
      {id, _user_id, _key} -> add_keys_after(filter, id, count + length(rows))
    end
  end

  @doc """
  Deletes cached lookups for keys the `KeyFilter` reports do not exist.
  """
  @spec sweep() :: non_neg_integer()
  def sweep do
    Cache
    |> Cachex.stream!(Cachex.Query.build(output: :key))
    |> Stream.filter(fn
      {:lookup, [user_id, key, _accessor_path]} -> not KeyFilter.maybe_exists?(user_id, key)
      _key -> false
    end)
    |> Enum.reduce(0, fn key, count ->
      {:ok, _} = Cachex.del(Cache, key)
      count + 1
    end)
  end

  @doc """
  Warms up to `limit` entries: keys with recorded usage, most recently used
  first, then keys never used, most recently updated first.
  """
  @spec warm_top_n(pos_integer()) :: {:ok, :ok} | {:error, term()}
  def warm_top_n(limit \\ @warm_limit) do
    Repo.transaction(fn ->
      warmed = limit |> recently_used_query() |> put_all()
      remaining = limit - warmed
      if remaining > 0, do: remaining |> never_used_query() |> put_all()
      :ok
    end)
  end

  defp recently_used_query(limit) do
    KeyValueUsage
    |> join(:inner, [u], kv in assoc(u, :key_value))
    |> order_by([u], desc: u.last_used_at)
    |> limit(^limit)
    |> select([_u, kv], kv)
  end

  defp never_used_query(limit) do
    used = where(KeyValueUsage, [u], u.key_value_id == parent_as(:kv).id)

    from(kv in KeyValue, as: :kv)
    |> where(not exists(used))
    |> order_by(desc: :updated_at)
    |> limit(^limit)
  end

  defp put_all(query) do
    query
    |> Repo.stream()
    |> Stream.chunk_every(500)
    |> Enum.reduce(0, fn chunk, count ->
      Cachex.put_many(Cache, Enum.map(chunk, &to_cache_entry/1))
      count + length(chunk)
    end)
  end

  def warm_recent do
    key_values =
      KeyValue
      |> where([kv], kv.updated_at >= ago(1, "hour"))
      |> Repo.all()

    Enum.each(key_values, &KeyFilter.add(&1.user_id, &1.key))
    if key_values != [], do: Cachex.put_many(Cache, Enum.map(key_values, &to_cache_entry/1))
  end

  defp to_cache_entry(%KeyValue{} = kv) do
    {{:lookup, [kv.user_id, kv.key, nil]}, {:cached, kv.value}}
  end

  @doc """
  When the last full warm completed on this node, or `nil` if none has.

  Entries `put` by the warmer were written at or before this moment, so consumers
  of write-recency (like `Cache.touch_recent_usages/1`) can use it to tell warmed
  entries apart from genuine cache misses.
  """
  @spec warmed_at() :: DateTime.t() | nil
  def warmed_at do
    :persistent_term.get(@pt_warmed_at, nil)
  end

  defp initialized? do
    :persistent_term.get(@pt_key, false)
  end

  defp full_warm_due? do
    case warmed_at() do
      nil ->
        true

      warmed_at ->
        DateTime.diff(DateTime.utc_now(), warmed_at, :millisecond) >= @full_warm_interval
    end
  end
end
