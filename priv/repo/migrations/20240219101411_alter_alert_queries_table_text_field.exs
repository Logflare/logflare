defmodule Logflare.Repo.Migrations.AlterAlertQueriesTableTextField do
  use Ecto.Migration

  def change do
    alter table(:alert_queries) do
      modify :query, :text, from: :string
      modify :description, :text, from: :string
    end
  end
end
