defmodule Logflare.Repo.Migrations.AddDefaultIngestFlagToBackends do
  use Ecto.Migration

  def change do
    alter table(:backends) do
      add :default_ingest, :boolean, default: false
    end

    create index(:backends, [:source_id, :default_ingest],
             where: "default_ingest = TRUE",
             name: :idx_backends_default_ingest
           )
  end
end
