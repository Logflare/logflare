defmodule Logflare.Repo.Migrations.AddEnableSpoolingToSources do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :enable_spooling, :boolean, default: false
    end
  end
end
