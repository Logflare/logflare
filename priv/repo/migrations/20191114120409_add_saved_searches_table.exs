defmodule Logflare.Repo.Migrations.AddSavedSearchesTable do
  use Ecto.Migration

  def change do
    create table(:saved_searches) do
      add :querystring, :string
      add :source_id, references(:sources)

      timestamps()
    end

    create unique_index(:saved_searches, [:querystring, :source_id])
  end
end
