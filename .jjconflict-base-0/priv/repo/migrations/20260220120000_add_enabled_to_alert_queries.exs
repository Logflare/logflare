defmodule Logflare.Repo.Migrations.AddEnabledToAlertQueries do
  use Ecto.Migration

  def change do
    alter table(:alert_queries) do
      add :enabled, :boolean, default: true, null: false
    end
  end
end
