defmodule Logflare.SourceSchemas.CacheWarmer do
  alias Logflare.ContextCache.Warmer
  alias Logflare.Repo
  alias Logflare.SourceSchemas.Cache
  alias Logflare.SourceSchemas.SourceSchema
  alias Logflare.Sources.Source
  import Ecto.Query

  use Cachex.Warmer
  @impl true
  def execute(_state), do: Warmer.warm(Cache, &warm/0)

  @spec warm() :: Warmer.pairs()
  defp warm do
    # Get source schemas for sources that have been active in the last day
    source_schemas =
      from(ss in SourceSchema,
        join: s in Source,
        on: ss.source_id == s.id,
        where: s.log_events_updated_at >= ago(1, "day"),
        order_by: {:desc, s.log_events_updated_at},
        limit: 1_000
      )
      |> Repo.all()

    for ss <- source_schemas do
      {{:get_source_schema_by, [[source_id: ss.source_id]]}, {:cached, ss}}
    end
  end
end
