defmodule Logflare.Backends.CacheWarmer do
  use Cachex.Warmer

  alias Logflare.Backends
  alias Logflare.ContextCache.Gossip

  @impl true
  def execute(_state) do
    cache_generation = Gossip.cache_invalidation_generation(Backends.Cache)
    backends = Backends.list_backends(ingesting: true, limit: 1_000)

    get_kv =
      for b <- backends do
        key = {:get_backend, [b.id]}
        value_generation = Gossip.cache_value_generation(Backends.Cache, key, b)
        {key, {:cached, b, value_generation}}
      end

    if cache_generation == Gossip.cache_invalidation_generation(Backends.Cache) do
      {:ok, get_kv}
    else
      {:ok, []}
    end
  end
end
