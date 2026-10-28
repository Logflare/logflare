defmodule Logflare.Repo.Migrations.UniqueDefaultIngestBackendPerUser do
  use Ecto.Migration

  def change do
    create unique_index(:backends, [:user_id, :type],
             where: "default_ingest = TRUE",
             name: :idx_backends_default_ingest_per_user_type
           )
  end
end
