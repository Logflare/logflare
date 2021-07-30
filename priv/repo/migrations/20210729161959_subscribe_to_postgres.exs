defmodule Logflare.Repo.Migrations.SubscribeToPostgres do
  use Ecto.Migration
  @disable_ddl_transaction true
  @disable_migration_lock true

  def up do
    # For Google Cloud SQL set cloudsql.logical_decoding to `on`
    # Give replication privileges to the `logflare` user on Google Cloud
    # ALTER USER logflare WITH REPLICATION;
    # https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication
    #
    # `max_replication_slots` defaults to 10 and scales for every gig of memory allocated

    env = Application.get_env(:logflare, :env)

    # Locally, make sure to restart Postgres after for these changes to take effect
    if env in [:dev, :test] do
      execute("ALTER SYSTEM SET wal_level = 'logical';")
    end

    execute("CREATE PUBLICATION logflare_pub FOR ALL TABLES;")
  end

  def down do
    # For Google Cloud SQL set cloudsql.logical_decoding to `off`

    env = Application.get_env(:logflare, :env)

    if env in [:dev, :test] do
      execute("ALTER SYSTEM RESET wal_level;")
    end

    execute("DROP PUBLICATION logflare_pub;")
  end
end
