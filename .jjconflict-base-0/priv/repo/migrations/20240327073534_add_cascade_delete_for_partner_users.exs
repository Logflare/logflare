defmodule Logflare.Repo.Migrations.AddCascadeDeleteForPartnerUsers do
  use Ecto.Migration

  def change do
    alter table(:partner_users) do
      modify :user_id, references(:users, on_delete: :delete_all), from: references(:users)
    end
  end
end
