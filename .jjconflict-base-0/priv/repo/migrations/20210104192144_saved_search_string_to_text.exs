defmodule Logflare.Repo.Migrations.SavedSearchStringToText do
  use Ecto.Migration

  def change do
    alter table(:saved_searches) do
      modify :querystring, :text
    end
  end
end
