defmodule Logflare.Repo.Migrations.AddDisableTailingToSourcesTable do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :disable_tailing, :boolean, default: false
    end
  end
end
