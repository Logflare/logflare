defmodule Logflare.Repo.Migrations.CreateTeamRoles do
  use Ecto.Migration

  def up do
    create table(:team_roles, primary_key: false) do
      add :role, :string, null: false
      add :team_user_id, references(:team_users, on_delete: :delete_all), null: false

      timestamps()
    end

    create unique_index(:team_roles, [:team_user_id])

    create constraint(:team_roles, :role_must_be_user_or_admin,
             check: "role IN ('user', 'admin')"
           )

    # Create team_role records for all existing team_users with role 'user'
    execute """
    INSERT INTO team_roles (team_user_id, role, inserted_at, updated_at)
    SELECT id, 'user', NOW(), NOW()
    FROM team_users
    """
  end

  def down do
    drop table(:team_roles)
  end
end
