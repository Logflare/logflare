defmodule Logflare.Backends.CacheWarmer do
  alias Logflare.Backends
  alias Logflare.Repo

  use Cachex.Warmer
  @impl true
  def execute(_state) do
    backends =
      Backends.list_backends(ingesting: true, limit: 1_000)
      |> Repo.preload(:sources)

    get_kv =
      for b <- backends do
        {{:get_backend, [b.id]}, {:cached, b}}
      end

    # Group backends by source_id to warm the `{:list_backends, [[source_id: id]]}` keys
    # used by the ingestion hot path (dispatch_to_backends, SourceSup.init, SourceSupWorker)
    list_by_source_kv =
      backends
      |> Enum.flat_map(fn b -> Enum.map(b.sources, &{&1.id, b}) end)
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
      |> Enum.map(fn {source_id, source_backends} ->
        # Strip preloaded sources to match what `list_backends(source_id:)` returns
        stripped = Enum.map(source_backends, &%{&1 | sources: %Ecto.Association.NotLoaded{}})
        {{:list_backends, [[source_id: source_id]]}, {:cached, stripped}}
      end)

    {:ok, get_kv ++ list_by_source_kv}
  end
end
