defmodule Logflare.Repo.Migrations.AddSavedSearchesSourceIdInsertedAtIndex do
  use Ecto.Migration

  def change do
    create index(:saved_searches, [:source_id, :inserted_at],
             where: "saved_by_user = true",
             name: :saved_searches_source_id_inserted_at_idx
           )
  end
end
