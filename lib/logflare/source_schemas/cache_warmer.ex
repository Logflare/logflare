defmodule Logflare.SourceSchemas.CacheWarmer do
  alias Logflare.SourceSchemas.SourceSchema
  alias Logflare.Repo
  alias Logflare.Source
  import Ecto.Query

  use Cachex.Warmer
  @impl true
  def execute(_state) do
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

    get_kv =
      for ss <- source_schemas do
        {:cached, {{:get_source_schema_by, [source_id: ss.source_id]}, ss}}
      end

    {:ok, get_kv}
  end
end
