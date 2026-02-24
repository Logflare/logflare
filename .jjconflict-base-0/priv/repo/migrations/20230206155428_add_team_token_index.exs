defmodule Logflare.Repo.Migrations.AddTeamTokenIndex do
  use Ecto.Migration

  def change do
    create unique_index(:teams, [:token])
  end
end
