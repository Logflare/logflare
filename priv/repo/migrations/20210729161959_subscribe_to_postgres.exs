defmodule Logflare.Repo.Migrations.SubscribeToPostgres do
  @moduledoc """
    Handles assigning user permissions for replication and creating the publications.

    Most importantly, it handles cleaning up named replication slots. If those are left on, and nothing connects
    to them they can get backed up and overload your Postgres instance.

    Make sure to restart Postgres after for these changes to take effect.

    For Google Cloud SQL set cloudsql.logical_decoding to `on` or `off` respectively.
    https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication
  """

  use Ecto.Migration
  @disable_ddl_transaction true
  @disable_migration_lock true
  @username Application.get_env(:logflare, Logflare.Repo)[:username]
  @slot Application.get_env(:logflare, Logflare.CacheBuster)[:replication_slot]
  @env Application.get_env(:logflare, :env)
  @publications Application.get_env(:logflare, Logflare.CacheBuster)[:publications]

  def up do
    if @env in [:dev, :test] do
      execute("ALTER SYSTEM SET wal_level = 'logical';")
    end

    execute("ALTER USER #{@username} WITH REPLICATION;")
    for p <- @publications, do: execute("CREATE PUBLICATION #{p} FOR ALL TABLES;")
  end

  def down do
    for p <- @publications, do: execute("DROP PUBLICATION #{p};")

    unless @slot == :temporary do
      execute("SELECT pg_drop_replication_slot('#{@slot}');")
    end

    execute("ALTER USER #{@username} WITH NOREPLICATION;")

    if @env in [:dev, :test] do
      execute("ALTER SYSTEM RESET wal_level;")
    end
  end
end
