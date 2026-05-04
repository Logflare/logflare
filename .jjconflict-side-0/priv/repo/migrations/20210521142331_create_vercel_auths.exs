defmodule Logflare.Repo.Migrations.CreateVercelAuths do
  use Ecto.Migration

  def change do
    create table(:vercel_auths) do
      add :access_token, :string
      add :installation_id, :string
      add :team_id, :string
      add :token_type, :string
      add :vercel_user_id, :string
      add :user_id, references(:users)

      timestamps()
    end

  end
end
