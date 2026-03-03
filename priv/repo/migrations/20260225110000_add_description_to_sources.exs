defmodule Logflare.Repo.Migrations.AddDescriptionToSources do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :description, :text
    end
  end
end
