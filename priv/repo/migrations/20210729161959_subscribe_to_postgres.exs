defmodule Logflare.Repo.Migrations.SubscribeToPostgres do
  use Ecto.Migration
  @disable_ddl_transaction true
  @disable_migration_lock true

  def up do
    # For Google Cloud SQL set cloudsql.logical_decoding to `on`

    env = Application.get_env(:logflare, :env)

    if env in [:dev, :test] do
      execute("ALTER SYSTEM SET wal_level = 'logical';")
    end

    execute("CREATE PUBLICATION logflare_pub FOR ALL TABLES;")
  end

  def down do
    # For Google Cloud SQL set cloudsql.logical_decoding to `off`

    env = Application.get_env(:logflare, :env)

    if env in [:dev, :test] do
      execute("ALTER SYSTEM SET wal_level = 'minimal';")
    end

    execute("DROP PUBLICATION logflare_pub;")
  end
end
