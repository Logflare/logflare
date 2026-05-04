defmodule Logflare.Repo.Migrations.DeleteSavedSearchesOnSourceDelete do
  use Ecto.Migration

  def up do
    drop(constraint(:saved_searches, "saved_searches_source_id_fkey"))

    alter table(:saved_searches) do
      modify(:source_id, references(:sources, on_delete: :delete_all))
    end
  end

  def down do
    drop(constraint(:saved_searches, "saved_searches_source_id_fkey"))

    alter table(:saved_searches) do
      modify(:source_id, references(:sources))
    end
  end
end
