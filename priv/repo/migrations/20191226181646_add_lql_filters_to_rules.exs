defmodule Logflare.Repo.Migrations.AddLqlFiltersToRules do
  use Ecto.Migration

  def change do
    alter table(:rules) do
      add :lql_filters, :map
    end
  end
end
