defmodule Logflare.Repo.Migrations.AddAlertWebhookHeaders do
  use Ecto.Migration

  def change do
    alter table(:alert_queries) do
      add(:webhook_notification_headers, :map)
    end
  end
end
