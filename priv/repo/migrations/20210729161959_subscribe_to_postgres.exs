defmodule Logflare.Repo.Migrations.SubscribeToPostgres do
  use Ecto.Migration
  @disable_ddl_transaction true
  @disable_migration_lock true

  def up do
    execute("ALTER SYSTEM SET wal_level = 'logical';")
    execute("CREATE PUBLICATION logflare_pub FOR ALL TABLES;")
  end

  def down do
    execute("ALTER SYSTEM SET wal_level = 'minimal';")
    execute("DROP PUBLICATION logflare_pub;")
  end
end
