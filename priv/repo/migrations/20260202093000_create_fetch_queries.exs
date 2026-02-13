defmodule Logflare.Repo.Migrations.CreateFetchQueries do
  use Ecto.Migration

  def change do
    create table(:fetch_queries) do
      add :name, :string, null: false
      add :description, :text
      add :external_id, :uuid, null: false, autogenerate: true
      add :language, :string, null: false, default: "bq_sql"
      add :query, :text
      add :cron, :string, null: false
      add :source_mapping, :map
      add :enabled, :boolean, null: false, default: true

      add :user_id, references(:users, on_delete: :delete_all), null: false
      add :backend_id, references(:backends, on_delete: :delete_all), null: true
      add :source_id, references(:sources, on_delete: :delete_all), null: false

      timestamps()
    end

    create index(:fetch_queries, [:user_id])
    create index(:fetch_queries, [:backend_id])
    create index(:fetch_queries, [:source_id])
    create index(:fetch_queries, [:enabled])
    create unique_index(:fetch_queries, [:external_id])
    create unique_index(:fetch_queries, [:user_id, :name])
  end
end
