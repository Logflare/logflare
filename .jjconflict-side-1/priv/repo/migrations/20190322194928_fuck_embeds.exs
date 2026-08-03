defmodule Logflare.Repo.Migrations.FuckEmbeds do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      remove(:other_email_notifications)
    end

    alter table(:sources) do
      add(:other_email_notifications, :string)
    end
  end
end
