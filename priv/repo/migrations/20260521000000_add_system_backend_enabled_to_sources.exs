defmodule Logflare.Repo.Migrations.AddSystemBackendEnabledToSources do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :system_backend_enabled, :boolean, null: false, default: true
    end
  end
end
