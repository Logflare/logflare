defmodule Logflare.Repo.Migrations.EndpointQueriesReplicaIdentityFull do
  use Ecto.Migration

  def up do
    execute("ALTER TABLE endpoint_queries REPLICA IDENTITY FULL")
  end

  def down do
    execute("ALTER TABLE endpoint_queries REPLICA IDENTITY DEFAULT")
  end
end
