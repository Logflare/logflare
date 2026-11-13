defmodule Logflare.Repo.Migrations.KeyValuesReplicaIdentityFull do
  use Ecto.Migration

  alias Logflare.Repo.Migrator

  def up do
    Migrator.with_replicated_execute(fn ->
      execute("ALTER TABLE key_values REPLICA IDENTITY FULL")
    end)
  end

  def down do
    Migrator.with_replicated_execute(fn ->
      execute("ALTER TABLE key_values REPLICA IDENTITY DEFAULT")
    end)
  end
end
