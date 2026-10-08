defmodule Logflare.ContextCache.RefreshAhead do
  @moduledoc """
  Refreshes hot cache entries in the background shortly before they expire, so that reads keep
  hitting the cache instead of blocking on the getter when an entry expires.

  Context caches opt in with `use Logflare.ContextCache, refresh_ahead: true`, which makes their
  `c:Logflare.ContextCache.fetch/2` call `maybe_refresh/3`. When the entry's remaining TTL drops
  below `:threshold` of its total TTL, the key is queued once. Entries that aren't read in that
  window expire as before.

  A single worker drains the queue at a constant rate, `:batch_size` keys every `:interval` ms,
  so refreshes don't burst into the database. For each batch it first asks up to `:max_peers`
  cluster peers for their entries in one call per peer. An entry is copied with the peer's
  remaining TTL when it is still outside the refresh window and not stale per
  `c:Logflare.ContextCache.stale_entry?/2`, so a cluster loads each hot key from the database
  about once per TTL rather than once per node. Keeping the peer's remaining TTL means a value is
  never cached for longer than one TTL after it was loaded from the database. The remaining keys
  are reloaded with their getters and written with their total TTL.

  Refreshed values are only written if the entry still exists, so a WAL bust that deletes the
  entry while it is queued or being refreshed isn't undone.

  Only `Logflare.ContextCache` callbacks are used, so it works with any
  `Logflare.ContextCache.Ops` implementation.
  """

  use GenServer

  require Logger

  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache

  @table __MODULE__
  @event [:logflare, :context_cache, :refresh_ahead]

  @type result :: :refreshed | :copied | :skipped | :failed

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_opts), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @spec maybe_refresh(module(), term(), (-> term())) :: :ok
  def maybe_refresh(cache, key, getter) do
    config = config()

    with true <- Keyword.get(config, :enabled, false),
         {remaining, total} <- cache.expiry(key),
         true <- remaining < total * config[:threshold] do
      enqueue(cache, key, getter, total, config[:max_queue])
    end

    :ok
  end

  @doc """
  Entries cached on this node under `keys`. Called by peers over RPC.
  """
  @spec peer_entries(module(), [term()]) :: [ContextCache.entry()]
  def peer_entries(cache, keys) do
    Enum.flat_map(keys, fn key -> List.wrap(cache.entry(key)) end)
  end

  @impl GenServer
  def init(_opts) do
    :ets.new(@table, [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    schedule_tick(config())
    {:ok, nil}
  end

  @impl GenServer
  def handle_info(:tick, state) do
    config = config()
    refresh_batch(config)
    schedule_tick(config)
    {:noreply, state}
  end

  defp config, do: Application.get_env(:logflare, __MODULE__, [])

  defp schedule_tick(config), do: Process.send_after(self(), :tick, config[:interval])

  defp enqueue(cache, key, getter, total, max_queue) do
    :ets.info(@table, :size) < max_queue and
      :ets.insert_new(@table, {{cache, key}, getter, total})
  rescue
    ArgumentError -> false
  end

  defp refresh_batch(config) do
    case :ets.match_object(@table, :_, config[:batch_size]) do
      {queued, _continuation} ->
        queued
        |> Enum.group_by(fn {{cache, _key}, _getter, _total} -> cache end)
        |> Enum.each(fn {cache, queued} -> refresh_cache(cache, queued, config) end)

      :"$end_of_table" ->
        :ok
    end
  end

  defp refresh_cache(cache, queued, config) do
    keys = Enum.map(queued, fn {{_cache, key}, _getter, _total} -> key end)
    peer_entries = fetch_peer_entries(cache, keys, config)

    for {{_cache, key} = id, getter, total} <- queued do
      started_at = System.monotonic_time()
      result = refresh(cache, key, getter, total, Map.get(peer_entries, key), config[:threshold])
      :ets.delete(@table, id)

      :telemetry.execute(@event, %{duration: System.monotonic_time() - started_at}, %{
        cache: cache,
        result: result
      })
    end
  end

  defp fetch_peer_entries(cache, keys, config) do
    case ClusterUtils.peer_list_partial(1.0, config[:max_peers]) do
      [] ->
        %{}

      peers ->
        peers
        |> :erpc.multicall(__MODULE__, :peer_entries, [cache, keys], config[:peer_timeout])
        |> Enum.flat_map(fn
          {:ok, entries} -> entries
          _error -> []
        end)
        |> Enum.reduce(%{}, &keep_longest_ttl/2)
    end
  end

  defp keep_longest_ttl({key, value, ttl}, acc) when is_integer(ttl) do
    case acc do
      %{^key => {_value, best_ttl}} when best_ttl >= ttl -> acc
      _ -> Map.put(acc, key, {value, ttl})
    end
  end

  defp keep_longest_ttl(_entry_without_ttl, acc), do: acc

  @spec refresh(
          module(),
          term(),
          (-> term()),
          pos_integer(),
          {term(), pos_integer()} | nil,
          float()
        ) ::
          result()
  defp refresh(cache, key, getter, total, peer_entry, threshold) do
    cond do
      not cache.cached?(key) ->
        :skipped

      copyable?(cache, key, peer_entry, total * threshold) ->
        {value, ttl} = peer_entry
        :ok = cache.put_entries([{key, value, ttl}])
        :copied

      true ->
        reload(cache, key, getter, total)
    end
  rescue
    e ->
      Logger.warning("Refresh-ahead failed for #{inspect(cache)}: #{Exception.message(e)}")
      :failed
  catch
    kind, reason ->
      Logger.warning("Refresh-ahead failed for #{inspect(cache)}: #{inspect({kind, reason})}")
      :failed
  end

  defp copyable?(_cache, _key, nil, _min_ttl), do: false

  defp copyable?(cache, key, {value, ttl}, min_ttl) do
    ttl > min_ttl and not cache.stale_entry?(key, value)
  end

  defp reload(cache, key, getter, total) do
    value = getter.()

    if cache.cached?(key) do
      :ok = cache.put_entries([{key, value, total}])
      :refreshed
    else
      :skipped
    end
  end
end
