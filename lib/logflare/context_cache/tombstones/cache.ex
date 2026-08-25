defmodule Logflare.ContextCache.Tombstones.Cache do
  @moduledoc false
  require Cachex.Spec

  @name __MODULE__
  @generation_ttl to_timeout(minute: 30)

  def child_spec(_options) do
    expiration =
      Cachex.Spec.expiration(
        interval: to_timeout(second: 30),
        default: to_timeout(minute: 1),
        lazy: true
      )

    hooks =
      if Application.get_env(:logflare, :cache_stats, false) do
        [Cachex.Spec.hook(module: Cachex.Stats)]
      end

    options = [
      expiration: expiration,
      hooks: List.wrap(hooks)
    ]

    Supervisor.child_spec({Cachex, [@name, options]}, id: @name)
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
