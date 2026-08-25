defmodule Logflare.ContextCache.Tombstones.Cache do
  @moduledoc false

  use Logflare.Cache

  alias Logflare.Cache.CachexOps

  @name __MODULE__
  @generation_ttl to_timeout(minute: 30)

  def child_spec(_options) do
    CachexOps.child_spec(@name,
      limit: nil,
      ttl: to_timeout(minute: 1),
      purge_interval: to_timeout(second: 30)
    )
  end

  def put_tombstone(cache, tombstone) do
    Cachex.put(
      @name,
      {cache, {:invalidation_generation, :cache}},
      make_ref(),
      expire: @generation_ttl
    )

    Cachex.put(
      @name,
      {cache, {:invalidation_generation, tombstone}},
      make_ref(),
      expire: @generation_ttl
    )

    Cachex.put(@name, {cache, tombstone}, make_ref())
  end

  def tombstoned?(cache, tombstone) do
    Cachex.exists?(@name, {cache, tombstone}) == {:ok, true}
  end

  @spec invalidation_generation(Cachex.t(), term()) :: reference() | nil
  def invalidation_generation(cache, tombstone) do
    Cachex.get!(@name, {cache, {:invalidation_generation, tombstone}})
  end

  @spec invalidation_generation(Cachex.t()) :: reference() | nil
  def invalidation_generation(cache) do
    Cachex.get!(@name, {cache, {:invalidation_generation, :cache}})
  end
end
