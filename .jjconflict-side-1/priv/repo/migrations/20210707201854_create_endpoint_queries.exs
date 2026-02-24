defmodule Logflare.Repo.Migrations.CreateEndpointQueries do
  use Ecto.Migration

  def change do
    create table(:endpoint_queries) do
      add :title, :string
      add :token, :uuid
      add :query, :text
      add :user_id, references(:users, on_delete: :nothing)

      timestamps()
    end

    create index(:endpoint_queries, [:user_id])
    create unique_index(:endpoint_queries, [:token])
  end
end
