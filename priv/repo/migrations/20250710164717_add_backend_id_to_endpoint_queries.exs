defmodule Logflare.Repo.Migrations.AddBackendIdToEndpointQueries do
  use Ecto.Migration

  def change do
    alter table(:endpoint_queries) do
      add :backend_id, references(:backends, on_delete: :nilify_all), null: true
    end

    create index(:endpoint_queries, [:backend_id])
  end
end
