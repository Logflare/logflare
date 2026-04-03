defmodule Logflare.ContextCache.Tombstones.Cache do
  @moduledoc false
  require Cachex.Spec

  @name __MODULE__

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

  def put_tombstone(tombstone) do
    Cachex.put(@name, tombstone, true)
  end

  def tombstoned?(tombstone) do
    Cachex.exists?(@name, tombstone) == {:ok, true}
  end
end
