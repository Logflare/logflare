defmodule Logflare.Sources.CacheWarmer do
  alias Logflare.Repo
  alias Logflare.Source
  import Ecto.Query

  use Cachex.Warmer
  @impl true
  def execute(_state) do
    # Get sources that have been active in the last day, similar to ingesting users pattern
    sources =
      from(s in Source,
        where: s.log_events_updated_at >= ago(1, "day"),
        order_by: {:desc, s.log_events_updated_at},
        limit: 1_000,
        preload: [:rules]
      )
      |> Repo.all()

    get_kv =
      for s <- sources do
        [
          {{:get_by, [id: s.id]}, s},
          {{:get_by, [token: s.token]}, s},
          {{:get_by_and_preload_rules, [id: s.id, user_id: s.user_id]}, s},
          {{:get_by_and_preload_rules, [token: s.token]}, s}
        ]
        |> Enum.map(&{:cached, &1})
      end

    {:ok, List.flatten(get_kv)}
  end
end
