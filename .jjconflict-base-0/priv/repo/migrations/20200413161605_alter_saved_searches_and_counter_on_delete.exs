defmodule Logflare.Repo.Migrations.AlterSavedSearchesAndCounterOnDelete do
  use Ecto.Migration

  def up do
    drop constraint(:saved_search_counters, "saved_search_counters_saved_search_id_fkey")

    alter table(:saved_search_counters) do
      modify :saved_search_id, references(:saved_searches, on_delete: :delete_all), null: false
    end
  end

  def down do
    drop constraint(:saved_searches_counters, "saved_search_counters_saved_search_id_fkey")

    alter table(:saved_search_counters) do
      modify :saved_search_id, references(:saved_searches, on_delete: :delete_all), null: false
    end
  end
end
