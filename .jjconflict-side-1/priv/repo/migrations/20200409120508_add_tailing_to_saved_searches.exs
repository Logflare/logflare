defmodule Logflare.Repo.Migrations.AddTailingToSavedSearches do
  use Ecto.Migration

  def change do
    alter table(:saved_searches) do
      add :tailing?, :boolean, null: false, default: true
    end
  end
end
