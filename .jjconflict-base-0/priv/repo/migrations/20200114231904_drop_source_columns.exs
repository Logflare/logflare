defmodule Logflare.Repo.Migrations.DropSourceColumns do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      remove(:user_email_notifications)
      remove(:user_text_notifications)
      remove(:other_email_notifications)
    end
  end
end
