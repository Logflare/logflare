defmodule Logflare.ContextCache.Tombstones.Cache do
  @moduledoc false

  use Logflare.Cache

  alias Logflare.Cache.CachexOps

  @name __MODULE__

  def child_spec(_options) do
    CachexOps.child_spec(@name,
      limit: nil,
      ttl: to_timeout(minute: 1),
      purge_interval: to_timeout(second: 30)
    )
  end

  def put_tombstone(cache, tombstone) do
    Cachex.put(@name, {cache, tombstone}, true)
  end

  def tombstoned?(cache, tombstone) do
    Cachex.exists?(@name, {cache, tombstone}) == {:ok, true}
  end
end
