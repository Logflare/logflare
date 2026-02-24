defmodule Logflare.Repo.Migrations.EmailNotificationsOnSource do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add(:user_email_notifications, :boolean, default: false, null: false)
      add(:other_email_notifications, :jsonb, default: "[]")
    end
  end
end
