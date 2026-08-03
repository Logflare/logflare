defmodule Logflare.Repo.Migrations.AddEnabledToBackends do
  use Ecto.Migration

  def change do
    alter table(:backends) do
      add :enabled, :boolean, default: true, null: false
    end
  end
end
