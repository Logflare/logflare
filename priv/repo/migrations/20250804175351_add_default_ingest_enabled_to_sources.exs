defmodule Logflare.Repo.Migrations.AddDefaultIngestEnabledToSources do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :default_ingest_enabled, :boolean, default: false
    end
  end
end
