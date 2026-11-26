defmodule Logflare.KeyValues.CacheWarmer do
  @moduledoc false

  use Cachex.Warmer

  import Ecto.Query

  require Logger

  alias Logflare.KeyValues.Cache
  alias Logflare.KeyValues.KeyFilter
  alias Logflare.KeyValues.KeyValue
  alias Logflare.Repo

  @pt_warmed_at {__MODULE__, :warmed_at}
  @full_warm_interval to_timeout(hour: 12)

  @impl true
  def execute(_state) do
    if full_warm_due?() do
      full_warm()
    else
      Repo.apply_with_replica(__MODULE__, :warm_recent, [])
    end

    :ignore
  end

  defp full_warm do
    started_at = System.system_time(:millisecond)
    {:ok, _count} = Repo.apply_with_replica(__MODULE__, :warm_all, [])
    sweep(started_at)
    :persistent_term.put(@pt_warmed_at, DateTime.utc_now())
  rescue
    e ->
      Logger.error("Error performing full KeyValues.Cache warming: #{inspect(e)}")
  end

  @doc """
  Loads every key value into the cache and rebuilds the `KeyFilter` from the same rows.
  """
  @spec warm_all() :: {:ok, non_neg_integer()} | {:error, term()}
  def warm_all do
    Repo.transaction(
      fn ->
        KeyValue
        |> Repo.aggregate(:count)
        |> KeyFilter.rebuild(&put_all/1)
      end,
      timeout: :infinity
    )
  end

  defp put_all(filter) do
    KeyValue
    |> Repo.stream()
    |> Stream.chunk_every(500)
    |> Enum.reduce(0, fn chunk, count ->
      Cachex.put_many(Cache, Enum.map(chunk, &to_cache_entry/1))
      KeyFilter.add_all(filter, Enum.map(chunk, &{&1.user_id, &1.key}))
      count + length(chunk)
    end)
  end

  @doc """
  Deletes lookup entries written before `started_at`, in ms. Run after a full warm, these are keys
  that no longer exist and derived entries, which are rebuilt from the warmed values.
  """
  @spec sweep(integer()) :: non_neg_integer()
  def sweep(started_at) do
    query = Cachex.Query.build(where: {:<, :modified, started_at}, output: :key)

    Cache
    |> Cachex.stream!(query)
    |> Stream.filter(&match?({:lookup, _}, &1))
    |> Enum.reduce(0, fn key, count ->
      {:ok, _} = Cachex.del(Cache, key)
      count + 1
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

  defp full_warm_due? do
    case warmed_at() do
      nil ->
        true

      warmed_at ->
        DateTime.diff(DateTime.utc_now(), warmed_at, :millisecond) >= @full_warm_interval
    end
  end
end
