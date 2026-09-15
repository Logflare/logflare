defmodule Logflare.Repo.Migrations.AddRoleToTeamUsers do
  use Ecto.Migration

  def up do
    alter table(:team_users) do
      add :role, :string, null: false, default: "user"
    end

    create constraint(:team_users, :team_users_role_must_be_user_or_admin,
             check: "role IN ('user', 'admin')"
           )
  end

  def down do
    drop constraint(:team_users, :team_users_role_must_be_user_or_admin)

    alter table(:team_users) do
      remove :role
    end
  end
end
