defmodule Logflare.Repo.Migrations.AlterRulesModifyLqlFiltersBytea do
  use Ecto.Migration

  def change do
    alter table(:rules) do
      remove :lql_filters
      add :lql_filters, :bytea
    end
  end
end
