defmodule Logflare.Repo.Migrations.AddLqlStringToRules do
  use Ecto.Migration

  def change do
    alter table(:rules) do
      add :lql_string, :text, default: "", null: false
    end
  end
end
