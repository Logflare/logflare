defmodule Logflare.Repo.Migrations.AddSchemaLockToSource do
  use Ecto.Migration
  def change do
    alter table(:sources) do
      add :lock_schema, :boolean, default: false, nullable: false
    end
  end
end
