defmodule Logflare.ContextCache.RefreshAhead do
  @moduledoc """
  Refreshes hot cache entries in the background shortly before they expire, so that reads keep
  hitting the cache instead of blocking on the getter when an entry expires.

  Context caches opt in with `use Logflare.ContextCache, refresh_ahead: true`, which makes their
  `c:Logflare.ContextCache.fetch/2` call `maybe_refresh/3`. When the entry's remaining TTL drops
  below `:threshold` of its total TTL, the key is queued once. Entries that aren't read in that
  window expire as before.

  A single worker drains the queue at a constant rate, `:batch_size` keys every `:interval` ms,
  so refreshes don't burst into the database. A queued key is skipped when its entry has left the
  refresh window in the meantime, for example because a read-through miss or a warmer replaced it.
  Otherwise the getter that produced the entry is re-run and its result is written with the
  cache's default TTL. A getter returning `{:error, :database_unavailable}` leaves the cached value
  in place, as on a read-through miss.

  `Logflare.ContextCache.bust_keys/1` bumps the generation of queued keys before deleting them.
  A refresh is skipped when its key was busted after it was queued, and a refreshed value is only
  written if the entry still holds the value it had when the refresh started. A bust landing
  between that check and the write evicts the written value again.

  Only `Logflare.ContextCache` callbacks are used, so it works with any
  `Logflare.ContextCache.Ops` implementation.
  """

  use GenServer

  require Logger

  @table __MODULE__
  @event [:logflare, :context_cache, :refresh_ahead]

  @type result :: :refreshed | :skipped | :failed

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_opts), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @spec maybe_refresh(module(), term(), (-> term())) :: :ok
  def maybe_refresh(cache, key, getter) do
    config = config()

    with true <- Keyword.get(config, :enabled, false),
         true <- in_refresh_window?(cache, key, config[:threshold]) do
      enqueue(cache, key, getter, config[:max_queue])
    end

    :ok
  end

  @doc """
  Bumps the generation of queued refreshes of `keys`, so their results are discarded.
  Must be called before the keys are deleted.
  """
  @spec mark_busted(module(), Enumerable.t()) :: :ok
  def mark_busted(cache, keys), do: Enum.each(keys, &bump_generation({cache, &1}))

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

  defp in_refresh_window?(cache, key, threshold) do
    case cache.expiry(key) do
      {remaining, total} -> remaining < total * threshold
      nil -> false
    end
  end

  defp enqueue(cache, key, getter, max_queue) do
    :ets.info(@table, :size) < max_queue and
      :ets.insert_new(@table, {{cache, key}, getter, 0})
  rescue
    ArgumentError -> false
  end

  defp bump_generation(id) do
    if :ets.member(@table, id), do: :ets.update_counter(@table, id, {3, 1})
  rescue
    ArgumentError -> nil
  end

  defp busted?(cache, key), do: :ets.lookup_element(@table, {cache, key}, 3, 0) > 0

  defp refresh_batch(config) do
    case :ets.match_object(@table, :_, config[:batch_size]) do
      {queued, _continuation} -> Enum.each(queued, &refresh_queued(&1, config[:threshold]))
      :"$end_of_table" -> :ok
    end
  end

  defp refresh_queued({{cache, key} = id, getter, _generation}, threshold) do
    started_at = System.monotonic_time()
    result = refresh(cache, key, getter, threshold)
    :ets.delete(@table, id)

    :telemetry.execute(@event, %{duration: System.monotonic_time() - started_at}, %{
      cache: cache,
      result: result
    })
  end

  @spec refresh(module(), term(), (-> term()), float()) :: result()
  defp refresh(cache, key, getter, threshold) do
    with false <- busted?(cache, key),
         true <- in_refresh_window?(cache, key, threshold),
         {:cached, _value} = snapshot <- cached_value(cache, key) do
      reload(cache, key, snapshot, getter.())
    else
      _not_due -> :skipped
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

  defp cached_value(cache, key) do
    case cache.entry(key) do
      {_key, value, _ttl} -> {:cached, value}
      nil -> :missing
    end
  end

  defp reload(cache, _key, _snapshot, {:error, :database_unavailable}) do
    Logger.warning("Refresh-ahead for #{inspect(cache)} could not reach the database")
    :failed
  end

  defp reload(cache, key, snapshot, value) do
    if cached_value(cache, key) == snapshot do
      :ok = cache.put_entries([{key, value, nil}])
      discard_if_busted(cache, key)
    else
      :skipped
    end
  end

  defp discard_if_busted(cache, key) do
    if busted?(cache, key) do
      {:ok, _deleted} = cache.delete_keys([key])
      :skipped
    else
      :refreshed
    end
  end
end
