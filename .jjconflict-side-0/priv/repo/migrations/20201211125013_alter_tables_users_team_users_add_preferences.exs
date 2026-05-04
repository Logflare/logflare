defmodule Logflare.Repo.Migrations.AlterTablesUsersTeamUsersAddPreferences do
  use Ecto.Migration

  def change do
    alter table(:team_users) do
      add :preferences, :map
    end

    alter table(:users) do
      add :preferences, :map
    end
  end
end
