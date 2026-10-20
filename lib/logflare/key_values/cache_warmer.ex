defmodule Logflare.KeyValues.CacheWarmer do
  @moduledoc false

  use Cachex.Warmer

  alias Logflare.ContextCache.PeerWarmer
  alias Logflare.KeyValues.Cache
  alias Logflare.KeyValues.KeyValue
  alias Logflare.Repo

  require Logger
  import Ecto.Query

  @pt_key {__MODULE__, :initialized}
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
        :fallback -> warm_full_from_db()
      end

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

  defp warm_full_from_db do
    started_at = DateTime.utc_now()
    Repo.apply_with_replica(__MODULE__, :warm_full, [])
    started_at
  end

  def warm_full do
    Repo.transaction(fn ->
      KeyValue
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

  defp initialized? do
    :persistent_term.get(@pt_key, false)
  end
end
