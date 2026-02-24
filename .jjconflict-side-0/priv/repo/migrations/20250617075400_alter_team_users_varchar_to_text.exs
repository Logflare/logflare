defmodule Logflare.Repo.Migrations.AlterTeamUsersVarcharToText do
  use Ecto.Migration

  def change do
    alter table(:team_users) do
      modify :provider_uid, :text, from: :string, null: true
      modify :token, :text, from: :string, null: true
    end
  end
end
