defmodule Logflare.Backends.CacheWarmer do
  alias Logflare.Backends

  use Cachex.Warmer
  @impl true
  def execute(_state) do
    backends = Backends.list_backends(ingesting: true, limit: 1_000)

    get_kv =
      for b <- backends do
        {{:get_backend, [b.id]}, {:cached, b}}
      end

    {:ok, get_kv}
  end
end
