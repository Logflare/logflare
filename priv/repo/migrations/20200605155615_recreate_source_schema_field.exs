defmodule Logflare.Repo.Migrations.RecreateSourceSchemaField do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      remove(:schema, :binary)
    end

    alter table(:sources) do
      add :schema, :binary
    end
  end
end
