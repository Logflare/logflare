defmodule Logflare.Repo.Migrations.CreateSourceSchemas do
  use Ecto.Migration

  def change do
    create table(:source_schemas) do
      add :bigquery_schema, :binary
      add :source_id, references(:sources, on_delete: :delete_all)

      timestamps()
    end

    create index(:source_schemas, [:source_id])
  end
end
