defmodule Logflare.Repo.Migrations.BackendsReplicaIdentityFull do
  use Ecto.Migration

  alias Logflare.Repo.Migrator

  def up do
    Migrator.with_replicated_execute(fn ->
      execute("ALTER TABLE backends REPLICA IDENTITY FULL")
    end)
  end

  def down do
    Migrator.with_replicated_execute(fn ->
      execute("ALTER TABLE backends REPLICA IDENTITY DEFAULT")
    end)
  end
end
