defmodule Logflare.Repo.Migrations.UpdateRulesLqlFiltersDefaultValue do
  use Ecto.Migration

  def change do
    alter table(:rules) do
      modify :lql_filters, :map, default: fragment("'[]'::JSONB"), null: false
    end
  end
end
