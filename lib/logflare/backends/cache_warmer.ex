defmodule Logflare.Backends.CacheWarmer do
  use Cachex.Warmer

  alias Logflare.Backends
  alias Logflare.ContextCache.Gossip

  @impl true
  def execute(_state) do
    generation = Gossip.cache_invalidation_generation(Backends.Cache)
    backends = Backends.list_backends(ingesting: true, limit: 1_000)

    get_kv =
      for b <- backends do
        key = {:get_backend, [b.id]}
        generation = Gossip.cache_invalidation_generation(Backends.Cache, key)
        {key, {:cached, b, generation}}
      end

    if generation == Gossip.cache_invalidation_generation(Backends.Cache) do
      {:ok, get_kv}
    else
      {:ok, []}
    end
  end
end
