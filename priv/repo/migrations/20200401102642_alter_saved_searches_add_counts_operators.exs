defmodule Logflare.Repo.Migrations.AlterSavedSearchesAddCountsOperators do
  use Ecto.Migration

  def change do
    alter table(:saved_searches) do
      add :saved_by_user, :boolean
      add :lql_filters, :jsonb
      add :lql_charts, :jsonb
    end
  end
end
