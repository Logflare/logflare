defmodule Logflare.Backends.CacheWarmer do
  require Logger

  alias Logflare.Backends
  alias Logflare.ContextCache.WriteFence

  use Cachex.Warmer

  @impl true
  def execute(_state) do
    {:ok, snapshot} = WriteFence.begin_snapshot(Backends)
    backends = Backends.list_backends(ingesting: true, limit: 1_000)

    entry_groups =
      for backend <- backends do
        {backend.id, [{{:get_backend, [backend.id]}, {:cached, backend}}]}
      end

    case WriteFence.commit(snapshot, Backends.Cache, entry_groups) do
      {:ok, %{skipped: skipped}} ->
        if skipped > 0 do
          Logger.warning("Backends cache warmer skipped #{skipped} invalidated backends")
        end

      {:error, reason} ->
        Logger.warning("Backends cache warmer could not commit: #{inspect(reason)}")
    end

    :ignore
  end
end
