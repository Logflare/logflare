defmodule Logflare.Repo.Migrations.AddDisableSystemDefaultBackendToSources do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :disable_system_default_backend, :boolean, default: false
    end
  end
end
