defmodule Logflare.ContextCache.RefreshAhead do
  @moduledoc """
  Refreshes hot cache entries in the background shortly before they expire, so that requests
  keep hitting the cache instead of blocking on a read-through database query at expiry.

  `Logflare.ContextCache.fetch/3` calls `maybe_refresh/3` on every cache hit. When the remaining
  TTL of the entry drops below `:threshold` of its full TTL, the getter that originally produced
  the entry is re-run in a supervised task and its result replaces the entry, resetting the TTL.
  Entries that aren't read in that window expire as before.

  At most one refresh runs per key, and at most `:max_concurrency` refreshes run in total.
  A refreshed value is only written if the entry still exists, so a WAL bust that deletes the
  entry while the refresh query is running isn't undone.
  """

  use GenServer

  import Cachex.Spec, only: [cache: 1, entry: 1]

  require Logger

  alias Logflare.Auth
  alias Logflare.Backends
  alias Logflare.Billing
  alias Logflare.Rules
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.Users
  alias Logflare.Utils.Tasks

  @table __MODULE__

  @caches [
    Auth.Cache,
    Backends.Cache,
    Billing.Cache,
    Rules.Cache,
    Sources.Cache,
    SourceSchemas.Cache,
    Users.Cache
  ]

  @type result :: :refreshed | :skipped | :failed

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_opts), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @spec caches() :: [atom()]
  def caches, do: @caches

  @spec maybe_refresh(Cachex.t(), term(), (-> term())) :: :ok
  def maybe_refresh(cache, key, getter_fn) do
    name = cache_name(cache)
    config = Application.get_env(:logflare, __MODULE__, [])

    if enabled?(config, name) and expiring?(name, key, config[:threshold]) and
         claim(name, key, config[:max_concurrency]) do
      start_refresh(name, key, getter_fn)
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

  defp enabled?(config, cache) do
    Keyword.get(config, :enabled, false) and cache in @caches
  end

  defp expiring?(cache, key, threshold) do
    case Cachex.inspect(cache, {:entry, key}) do
      {:ok, entry(modified: modified, expiration: expiration)} when is_integer(expiration) ->
        modified + expiration - System.system_time(:millisecond) < expiration * threshold

      _ ->
        false
    end
  end

  defp claim(cache, key, max_concurrency) do
    :ets.info(@table, :size) < max_concurrency and :ets.insert_new(@table, {{cache, key}})
  end

  defp release(cache, key), do: :ets.delete(@table, {cache, key})

  defp start_refresh(cache, key, getter_fn) do
    case Tasks.start_child(fn -> refresh(cache, key, getter_fn) end) do
      {:ok, _pid} -> :ok
      _error -> release(cache, key)
    end
  end

  defp refresh(cache, key, getter_fn) do
    :telemetry.span([:logflare, :context_cache, :refresh_ahead], %{cache: cache}, fn ->
      result = do_refresh(cache, key, getter_fn)
      {result, %{cache: cache, result: result}}
    end)
  after
    release(cache, key)
  end

  @spec do_refresh(atom(), term(), (-> term())) :: result()
  defp do_refresh(cache, key, getter_fn) do
    value = getter_fn.()

    if Cachex.exists?(cache, key) == {:ok, true} do
      Cachex.put(cache, key, {:cached, value})
      :refreshed
    else
      :skipped
    end
  rescue
    e ->
      Logger.warning("Refresh-ahead failed for #{inspect(cache)}: #{Exception.message(e)}")
      :failed
  end

  defp cache_name(cache(name: name)), do: name
  defp cache_name(name) when is_atom(name), do: name
end
