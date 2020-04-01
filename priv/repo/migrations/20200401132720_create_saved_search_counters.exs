defmodule Logflare.Repo.Migrations.CreateSavedSearchCounters do
  use Ecto.Migration

  def change do
    create table(:saved_search_counters) do
      add :datetime, :timestamp, null: false
      add :saved_search_id, references(:saved_searches)
      add :granularity, :text, default: "day", null: false
      add :non_tailing_count, :integer
      add :tailing_count, :integer
    end

    create unique_index(:saved_search_counters, [:datetime, :saved_search_id, :granularity])
  end
end
