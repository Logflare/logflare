defmodule Logflare.Repo.Migrations.AddSuggestedFieldsToSources do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add(:suggested_fields, :string)
    end
  end
end
