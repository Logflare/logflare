defmodule Logflare.Repo.Migrations.UniqueSourceSchemaIndex do
  use Ecto.Migration

  def change do
    drop index(:source_schemas, [:source_id])
    create unique_index(:source_schemas, [:source_id])
  end
end
