defmodule Logflare.Repo.Migrations.CreateIndexForUsersPartnersUpgraded do
  use Ecto.Migration

  def change do
    create index(:partner_users,[:partner_id, :user_id, :upgraded])
  end
end
