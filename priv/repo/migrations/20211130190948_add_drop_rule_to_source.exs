defmodule Logflare.Repo.Migrations.AddDropRuleToSource do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :drop_lql_filters, :bytea, null: false, default: "\\x836a"
      add :drop_lql_string, :string
    end
  end
end
