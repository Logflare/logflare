defmodule Logflare.Repo.Migrations.AlterUsersVarcharToText do
  use Ecto.Migration

  def change do
    alter table(:users) do
      modify :provider_uid, :text, from: :string, null: false
      modify :token, :text, from: :string, null: false
    end
  end
end
