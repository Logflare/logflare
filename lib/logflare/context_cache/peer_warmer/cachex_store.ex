defmodule Logflare.ContextCache.PeerWarmer.CachexStore do
  @moduledoc """
  `Logflare.ContextCache.PeerWarmer.Store` for plain Cachex caches.

  Entries are transferred as Cachex entry records and written with `Cachex.import/2`,
  which keeps the remaining TTL of every entry.
  """

  @behaviour Logflare.ContextCache.PeerWarmer.Store

  import Cachex.Spec

  @impl true
  def stream(cache), do: Cachex.stream!(cache)

  @impl true
  def key(entry(key: key)), do: key

  @impl true
  def value(entry(value: value)), do: value

  @impl true
  def exists?(cache, key), do: Cachex.exists?(cache, key) == {:ok, true}

  @impl true
  def put_entries(_cache, []), do: :ok

  def put_entries(cache, entries) do
    {:ok, _imported} = Cachex.import(cache, entries)
    :ok
  end

  @impl true
  def size(cache) do
    {:ok, size} = Cachex.size(cache)
    size
  end
end
