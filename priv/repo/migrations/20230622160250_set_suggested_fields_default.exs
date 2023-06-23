defmodule Logflare.Repo.Migrations.SetSuggestedFieldsDefault do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      modify(:suggested_keys, :string, default: "")
    end
  end
end
