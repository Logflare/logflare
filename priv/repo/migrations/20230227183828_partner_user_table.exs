defmodule Logflare.Repo.Migrations.PartnerUserTable do
  use Ecto.Migration

  def change do
    create table :partner_users do
      add :partner_id, references(:partners)
      add :user_id, references(:users)
    end

    create unique_index(:partner_users, [:partner_id, :user_id])
  end
end
