defmodule Logflare.Rules.CacheWarmer do
  alias Logflare.Repo
  alias Logflare.Sources.Source

  import Ecto.Query

  use Cachex.Warmer

  @impl true
  def execute(_state) do
    sources =
      from(s in Source,
        where: s.log_events_updated_at >= ago(2, "hour"),
        order_by: {:desc, s.log_events_updated_at},
        limit: 500,
        preload: :rules
      )
      |> Repo.all()

    entries =
      for s <- sources do
        {{:list_by_source_id, [s.id]}, {:cached, s.rules}}
      end

    {:ok, entries}
  end
end
