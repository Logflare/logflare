defmodule Logflare.Repo.Migrations.AddDefaultSearchLqlToSources do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :default_search_lql, :text
    end
  end
end
