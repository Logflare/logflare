defmodule Logflare.Repo.Migrations.AlertQueriesReplicaIdentityFull do
  use Ecto.Migration

  def up do
    execute("ALTER TABLE alert_queries REPLICA IDENTITY FULL")
  end

  def down do
    execute("ALTER TABLE alert_queries REPLICA IDENTITY DEFAULT")
  end
end
