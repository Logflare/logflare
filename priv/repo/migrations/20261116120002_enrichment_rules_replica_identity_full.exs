defmodule Logflare.Repo.Migrations.EnrichmentRulesReplicaIdentityFull do
  use Ecto.Migration

  alias Logflare.Repo.Migrator

  def up do
    Migrator.with_replicated_execute(fn ->
      execute("ALTER TABLE enrichment_rules REPLICA IDENTITY FULL")
    end)
  end

  def down do
    Migrator.with_replicated_execute(fn ->
      execute("ALTER TABLE enrichment_rules REPLICA IDENTITY DEFAULT")
    end)
  end
end
