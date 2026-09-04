defmodule Logflare.Rules.CacheWarmer do
  require Logger

  alias Logflare.ContextCache.WriteFence
  alias Logflare.Repo
  alias Logflare.Rules
  alias Logflare.Sources.Source

  import Ecto.Query

  use Cachex.Warmer

  @impl true
  def execute(_state) do
    {:ok, snapshot} = WriteFence.begin_snapshot(Rules)

    sources =
      from(s in Source,
        where: s.log_events_updated_at >= ago(2, "hour"),
        order_by: {:desc, s.log_events_updated_at},
        limit: 500,
        preload: :rules
      )
      |> Repo.all()

    entry_groups =
      for source <- sources do
        {:unknown, [{{:list_by_source_id, [source.id]}, {:cached, source.rules}}]}
      end

    case WriteFence.commit(snapshot, Rules.Cache, entry_groups) do
      {:ok, %{skipped: skipped}} ->
        if skipped > 0 do
          Logger.warning("Rules cache warmer skipped #{skipped} entries after invalidation")
        end

      {:error, reason} ->
        Logger.warning("Rules cache warmer could not commit: #{inspect(reason)}")
    end

    :ignore
  end
end
