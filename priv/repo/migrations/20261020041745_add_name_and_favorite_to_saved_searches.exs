defmodule Logflare.Repo.Migrations.AddNameAndFavoriteToSavedSearches do
  use Ecto.Migration

  def change do
    alter table(:saved_searches) do
      add :name, :string
      add :favorite, :boolean, default: false, null: false
    end
  end
end
