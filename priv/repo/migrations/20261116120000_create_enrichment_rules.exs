defmodule Logflare.Repo.Migrations.CreateEnrichmentRules do
  use Ecto.Migration

  def change do
    create table(:enrichment_rules) do
      add :token, :uuid, null: false
      add :source_id, references(:sources, on_delete: :delete_all), null: false
      add :lql_string, :text, null: false
      add :lql_filters, :binary, null: false
      add :set_fields, :map, null: false

      timestamps()
    end

    create index(:enrichment_rules, [:source_id])
    create unique_index(:enrichment_rules, [:token])
  end
end
