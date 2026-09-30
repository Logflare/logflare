defmodule Logflare.ContextCache.RefreshAhead do
  @moduledoc """
  Refreshes hot cache entries in the background shortly before they expire, so that reads keep
  hitting the cache instead of blocking on the getter when an entry expires.

  Context caches opt in with `use Logflare.ContextCache, refresh_ahead: true`, which makes their
  `c:Logflare.ContextCache.fetch/2` call `maybe_refresh/3`. When the entry's remaining TTL drops
  below `:threshold` of its total TTL, the getter that produced the entry is re-run in a supervised
  task and its result replaces the entry with the same total TTL. Entries that aren't read in that
  window expire as before.

  Only the `Logflare.ContextCache` callbacks `c:Logflare.ContextCache.expiry/1`,
  `c:Logflare.ContextCache.cached?/1` and `c:Logflare.ContextCache.put_entries/1` are used, so it
  works with any `Logflare.ContextCache.Ops` implementation.

  At most one refresh runs per key, and at most `:max_concurrency` refreshes run in total.
  A refreshed value is only written if the entry still exists, so a WAL bust that deletes the
  entry while the refresh is running isn't undone.
  """

  use GenServer

  require Logger

  alias Logflare.Utils.Tasks

  @table __MODULE__

  @type result :: :refreshed | :skipped | :failed

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_opts), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @spec maybe_refresh(module(), term(), (-> term())) :: :ok
  def maybe_refresh(cache, key, getter) do
    config = Application.get_env(:logflare, __MODULE__, [])

    with true <- Keyword.get(config, :enabled, false),
         {remaining, total} <- cache.expiry(key),
         true <- remaining < total * config[:threshold],
         true <- claim(cache, key, config[:max_concurrency]) do
      start_refresh(cache, key, getter, total)
    end

    :ok
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

    {:ok, nil}
  end

  defp claim(cache, key, max_concurrency) do
    :ets.info(@table, :size) < max_concurrency and :ets.insert_new(@table, {{cache, key}})
  end

  defp release(cache, key), do: :ets.delete(@table, {cache, key})

  defp start_refresh(cache, key, getter, ttl) do
    case Tasks.start_child(fn -> refresh(cache, key, getter, ttl) end) do
      {:ok, _pid} -> :ok
      _error -> release(cache, key)
    end
  end

  defp refresh(cache, key, getter, ttl) do
    :telemetry.span([:logflare, :context_cache, :refresh_ahead], %{cache: cache}, fn ->
      result = do_refresh(cache, key, getter, ttl)
      {result, %{cache: cache, result: result}}
    end)
  after
    release(cache, key)
  end

  @spec do_refresh(module(), term(), (-> term()), pos_integer()) :: result()
  defp do_refresh(cache, key, getter, ttl) do
    value = getter.()

    if cache.cached?(key) do
      :ok = cache.put_entries([{key, value, ttl}])
      :refreshed
    else
      :skipped
    end
  rescue
    e ->
      Logger.warning("Refresh-ahead failed for #{inspect(cache)}: #{Exception.message(e)}")
      :failed
  end
end
