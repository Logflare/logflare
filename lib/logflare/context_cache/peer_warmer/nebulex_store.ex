defmodule Logflare.ContextCache.PeerWarmer.NebulexStore do
  @moduledoc """
  `Logflare.ContextCache.PeerWarmer.Store` for a single Nebulex cache level.

  The target is the local level (e.g. `Logflare.KeyValues.Cache.L1`), not the multi-level
  cache, so copied entries are never written through to other levels.
  """

  @behaviour Logflare.ContextCache.PeerWarmer.Store

  @stream_buffer 500

  @impl true
  def stream(cache), do: cache.stream!([], max_entries: @stream_buffer)

  @impl true
  def key({key, _value}), do: key

  @impl true
  def value({_key, value}), do: value

  @impl true
  def exists?(cache, key), do: cache.has_key?(key) == {:ok, true}

  @impl true
  def put_entries(_cache, []), do: :ok
  def put_entries(cache, entries), do: cache.put_all!(entries)

  @impl true
  def size(cache), do: cache.count_all!()
end
