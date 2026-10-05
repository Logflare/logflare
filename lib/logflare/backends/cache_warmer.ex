defmodule Logflare.Backends.CacheWarmer do
  alias Logflare.Backends
  alias Logflare.Repo

  use Cachex.Warmer

  @impl true
  def execute(_state), do: Repo.with_replica(&warm/0)

  defp warm do
    backends = Backends.list_backends(ingesting: true, limit: 1_000)

    get_kv =
      for b <- backends do
        {{:get_backend, [b.id]}, {:cached, b}}
      end

    {:ok, get_kv}
  end
end
