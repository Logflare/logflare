defmodule Logflare.Repo.Migrations.AddTokenToTeam do
  use Ecto.Migration

  def change do
    alter table(:teams) do
      add :token, :string
    end
  end
end
