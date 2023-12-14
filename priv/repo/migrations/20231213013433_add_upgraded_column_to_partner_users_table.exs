defmodule Logflare.Repo.Migrations.AddUpgradedColumnToPartnerUsersTable do
  use Ecto.Migration

  def up do

    alter table(:partner_users) do
      add :upgraded, :boolean, default: false, null: false
    end
  end

  def down do

    alter table(:partner_users) do
      remove :upgraded
    end
  end
end
