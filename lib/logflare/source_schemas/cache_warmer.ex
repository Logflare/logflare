defmodule Logflare.SourceSchemas.CacheWarmer do
  require Logger

  alias Logflare.ContextCache.WriteFence
  alias Logflare.SourceSchemas
  alias Logflare.SourceSchemas.SourceSchema
  alias Logflare.Repo
  alias Logflare.Sources.Source
  import Ecto.Query

  use Cachex.Warmer
  @impl true
  def execute(_state) do
    {:ok, snapshot} = WriteFence.begin_snapshot(SourceSchemas)

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

    entry_groups =
      for source_schema <- source_schemas do
        {source_schema.id,
         [
           {{:get_source_schema_by, [[source_id: source_schema.source_id]]},
            {:cached, source_schema}}
         ]}
      end

    case WriteFence.commit(snapshot, SourceSchemas.Cache, entry_groups) do
      {:ok, %{skipped: skipped}} ->
        if skipped > 0 do
          Logger.warning("Source schemas cache warmer skipped #{skipped} invalidated schemas")
        end

      {:error, reason} ->
        Logger.warning("Source schemas cache warmer could not commit: #{inspect(reason)}")
    end

    :ignore
  end
end
