defmodule Logflare.Repo.Migrations.UpdateUserConstraints do
  use Ecto.Migration

  def change do
    alter table(:users) do
      modify :provider, :string, null: false
      modify :email, :string, null: false
      modify :token, :string, null: false
      modify :api_key, :string, null: false
    end
  end
end
