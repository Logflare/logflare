defmodule Logflare.KeyValues.CacheWarmer do
  @moduledoc false

  use Cachex.Warmer

  alias Logflare.ContextCache.PeerWarmer
  alias Logflare.KeyValues.Cache
  alias Logflare.KeyValues.KeyValue
  alias Logflare.KeyValues.KeyValueUsage
  alias Logflare.Repo

  require Logger
  import Ecto.Query

  @pt_key {__MODULE__, :initialized}
  @pt_warmed_at {__MODULE__, :warmed_at}
  @warm_limit Application.compile_env!(:logflare, [__MODULE__, :warm_limit])
  @catch_up_margin_sec 60

  @impl true
  def execute(_state) do
    if initialized?() do
      refresh_recent()
    else
      initial_warm()
    end

    :ignore
  end

  defp refresh_recent do
    started_at = DateTime.utc_now()
    Repo.apply_with_replica(__MODULE__, :warm_recent, [DateTime.add(started_at, -1, :hour)])
    PeerWarmer.mark_ready(Cache, started_at)
  end

  defp initial_warm do
    PeerWarmer.mark_warming(Cache)

    warmed_at =
      case PeerWarmer.copy_from_peer(Cache, @warm_limit) do
        {:ok, %{warmed_at: peer_warmed_at}} -> catch_up_since(peer_warmed_at)
        :fallback -> warm_top_n_from_db()
      end

    :persistent_term.put(@pt_warmed_at, DateTime.utc_now())
    :persistent_term.put(@pt_key, true)
    PeerWarmer.mark_ready(Cache, warmed_at)
  rescue
    e ->
      Logger.error("Error performing full KeyValues.Cache warming: #{inspect(e)}")
  end

  defp catch_up_since(peer_warmed_at) do
    started_at = DateTime.utc_now()
    since = DateTime.add(peer_warmed_at, -@catch_up_margin_sec, :second)
    Repo.apply_with_replica(__MODULE__, :warm_recent, [since])
    started_at
  end

  defp warm_top_n_from_db do
    started_at = DateTime.utc_now()
    Repo.apply_with_replica(__MODULE__, :warm_top_n, [])
    started_at
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

  @spec warm_recent(DateTime.t()) :: term()
  def warm_recent(since \\ DateTime.add(DateTime.utc_now(), -1, :hour)) do
    entries =
      KeyValue
      |> where([kv], kv.updated_at >= ^since)
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
