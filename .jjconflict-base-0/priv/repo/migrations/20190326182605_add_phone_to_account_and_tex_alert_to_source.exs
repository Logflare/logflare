defmodule Logflare.Repo.Migrations.AddPhoneToAccountAndTexAlertToSource do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add(:user_text_notifications, :boolean, default: false, null: false)
    end

    alter table(:users) do
      add(:phone, :string)
    end
  end
end
