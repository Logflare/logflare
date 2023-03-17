defmodule Logflare.Repo.Migrations.CreateAlertQueries do
  use Ecto.Migration

  def change do
    create table(:alert_queries) do
      add :name, :string
      add :token, :uuid
      add :query, :string
      add :cron, :string
      add :active, :boolean, default: true
      add :source_mapping, :map
      add :slack_hook_url, :string
      add :webhook_notification_url, :string
      add :user_id, references(:users, on_delete: :nothing)
      timestamps()
    end
    create index(:alert_queries, [:user_id])
    create unique_index(:alert_queries, [:token])
  end
end
