defmodule Logflare.Repo.Migrations.WebhookNotificationUrl do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add(:webhook_notification_url, :string)
    end
  end
end
