defmodule Logflare.KeyValues.CacheWarmer do
  @moduledoc false

  use Cachex.Warmer

  alias Logflare.KeyValues.Cache
  alias Logflare.KeyValues.KeyValue
  alias Logflare.KeyValues.KeyValueUsage
  alias Logflare.Repo

  require Logger
  import Ecto.Query

  @pt_key {__MODULE__, :initialized}
  @pt_warmed_at {__MODULE__, :warmed_at}

  @impl true
  def execute(_state) do
    if initialized?() do
      Repo.apply_with_replica(__MODULE__, :warm_recent, [])
    else
      try do
        Repo.apply_with_replica(__MODULE__, :warm_top_n, [])
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
  Warms up to `warm_limit` entries: keys with recorded usage, most recently used
  first, then keys never used, most recently inserted first.
  """
  @spec warm_top_n() :: {:ok, :ok} | {:error, term()}
  def warm_top_n do
    limit =
      Application.get_env(:logflare, __MODULE__, [])
      |> Keyword.get(:warm_limit, 500_000)

    Repo.transaction(fn ->
      warmed = limit |> recently_used_query() |> put_all()
      if warmed < limit, do: (limit - warmed) |> never_used_query() |> put_all()
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
    |> order_by(desc: :id)
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
    entries =
      KeyValue
      |> where([kv], kv.updated_at >= ago(1, "hour"))
      |> Repo.all()
      |> Enum.map(&to_cache_entry/1)

    if entries != [], do: Cachex.put_many(Cache, entries)
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
end
