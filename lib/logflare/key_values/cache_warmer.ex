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
      case PeerWarmer.copy_from_peer(Cache) do
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

  def warm_top_n do
    limit =
      Application.get_env(:logflare, __MODULE__, [])
      |> Keyword.get(:warm_limit, 500_000)

    ordered =
      KeyValue
      |> join(:left, [kv], u in KeyValueUsage, on: u.key_value_id == kv.id)
      |> order_by([kv, u], desc_nulls_last: u.last_used_at, desc: kv.updated_at)
      |> limit(^limit)

    Repo.transaction(fn ->
      ordered
      |> Repo.stream()
      |> Stream.chunk_every(500)
      |> Enum.each(fn chunk ->
        entries = Enum.map(chunk, &to_cache_entry/1)
        Cachex.put_many(Cache, entries)
      end)
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
