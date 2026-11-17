defmodule Logflare.Backends.CacheWarmer do
  alias Logflare.Backends
  alias Logflare.Backends.Cache
  alias Logflare.ContextCache.Warmer

  use Cachex.Warmer

  @impl true
  def execute(_state), do: Warmer.warm(Cache, &warm/0)

  @spec warm() :: Warmer.pairs()
  defp warm do
    backends = Backends.list_backends(ingesting: true, limit: 1_000)

    for b <- backends do
      {{:get_backend, [b.id]}, {:cached, b}}
    end
  end
end
