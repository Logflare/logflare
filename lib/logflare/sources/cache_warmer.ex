defmodule Logflare.Sources.CacheWarmer do
  alias Logflare.Repo
  alias Logflare.Sources.Source
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
        value = {:cached, s}

        [
          {{:get_by, [id: s.id]}, value},
          {{:get_by, [token: s.token]}, value},
          {{:get_by_and_preload_rules, [id: s.id, user_id: s.user_id]}, value},
          {{:get_by_and_preload_rules, [token: s.token]}, value}
        ]
      end

    {:ok, List.flatten(get_kv)}
  end
end
