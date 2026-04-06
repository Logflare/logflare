defmodule Logflare.Rules.CacheWarmer do
  alias Logflare.Repo
  alias Logflare.Sources.Source
  alias Logflare.Sources.SourceRouter.RulesTree

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

    # Also warm `{:rules_tree_by_source_id, [id]}` keys used by the ingestion hot path (SourceRouter.RulesTree.matching_rules)
    tree_entries =
      for s <- sources do
        tree = RulesTree.build(s.rules)
        {{:rules_tree_by_source_id, [s.id]}, {:cached, tree}}
      end

    {:ok, entries ++ tree_entries}
  end
end
