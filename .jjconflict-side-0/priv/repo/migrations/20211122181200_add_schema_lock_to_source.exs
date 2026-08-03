defmodule Logflare.Repo.Migrations.AddSchemaLockToSource do
  use Ecto.Migration
  def change do
    alter table(:sources) do
      add :lock_schema, :boolean, default: false, nullable: false
      add :validate_schema, :boolean, default: true, nullable: false
    end
  end
end
