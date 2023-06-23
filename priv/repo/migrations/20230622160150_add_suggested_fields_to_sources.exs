defmodule Logflare.Repo.Migrations.AddSuggestedFieldsToSources do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add(:suggested_keys, :string)
    end
  end
end
