defmodule Logflare.Repo.Migrations.AlterSavedSearchesAddCountsOperators do
  use Ecto.Migration

  def change do
    alter table(:saved_searches) do
      add :saved_by_user, :boolean
      add :lql, :jsonb
    end
  end
end
