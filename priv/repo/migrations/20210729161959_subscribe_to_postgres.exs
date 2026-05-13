defmodule Logflare.Repo.Migrations.SubscribeToPostgres do
  @moduledoc """
    Handles assigning user permissions for replication and creating the publications.

    Most importantly, it handles cleaning up named replication slots. If those are left on, and nothing connects
    to them they can get backed up and overload your Postgres instance.

    Make sure to restart Postgres after for these changes to take effect.

    For Google Cloud SQL set cloudsql.logical_decoding to `on` or `off` respectively.
    https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication

    Figure out how to do `execute("alter table "rules" replica identity full")` automatically for each table here and for ones created in the future.
  """

  use Ecto.Migration

  @disable_ddl_transaction true
  @disable_migration_lock true
  @username Application.compile_env(:logflare, Logflare.Repo)[:username]
  @slot Application.compile_env(:logflare, Logflare.ContextCache.CacheBuster)[:replication_slot]
  @publications Application.compile_env(:logflare, Logflare.ContextCache.CacheBuster)[:publications]
  @publication_tables [
    "billing_accounts",
    "plans",
    "rules",
    "source_schemas",
    "sources",
    "users"
  ]

  def up do
    if env() in [:dev, :test] do
      execute("ALTER SYSTEM SET wal_level = 'logical';")
      execute("ALTER USER #{@username} WITH REPLICATION;")
    end

    # execute on specific tables so that we don't need superuser privilege
    # to update the publication in the future, we rerun in another migration ALTER PUBLICATIONS .. FOR TABLE users, sources ...
    for p <- @publications do
      tables = Enum.join(@publication_tables, ", ")
      execute("CREATE PUBLICATION #{p} FOR TABLE #{tables};")
    end

    # This is happening in `20210810182003_set_rules_to_replica_identity_full.exs`
    # execute("alter table rules replica identity full")
  end

  def down do
    for p <- @publications, do: execute("DROP PUBLICATION #{p};")

    unless @slot == :temporary do
      execute("SELECT pg_drop_replication_slot('#{@slot}');")
    end

    # This is happening in `20210810182003_set_rules_to_replica_identity_full.exs`
    # execute("alter table rules replica identity default")

    if env() in [:dev, :test] do
      execute("ALTER USER #{@username} WITH NOREPLICATION;")
      execute("ALTER SYSTEM RESET wal_level;")
    end
  end

  defp env do
    Application.get_env(:logflare, :env)
  end
end
