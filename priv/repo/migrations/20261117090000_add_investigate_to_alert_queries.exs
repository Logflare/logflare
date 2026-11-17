defmodule Logflare.Repo.Migrations.AddInvestigateToAlertQueries do
  use Ecto.Migration

  def change do
    alter table(:alert_queries) do
      add :investigate, :boolean, null: false, default: false
    end
  end
end
