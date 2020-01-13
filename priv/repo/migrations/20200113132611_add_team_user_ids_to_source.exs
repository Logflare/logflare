defmodule Logflare.Repo.Migrations.AddTeamUserIdsToSource do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :notifications, :map, default: %{}, null: false
    end
  end
end
