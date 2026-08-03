defmodule Logflare.Repo.Migrations.AddTeamUserIdsToSource do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :notifications, :map, default: %Logflare.Sources.Source.Notifications{}, null: false
    end
  end

  def default() do
  end
end
