defmodule Logflare.Ecto.Adapters.PglogicalPostgres do
  @moduledoc """
  Ecto adapter identical to `Ecto.Adapters.Postgres`, except DDL statements
  emitted during migration are wrapped in `pglogical.replicate_ddl_command/2`
  (see `Logflare.Ecto.Adapters.PglogicalPostgres.Connection`) so they
  propagate to subscribed pglogical replication sets.

  Intended ONLY for use by `Logflare.Repo.Pglogical` during migrations;
  never used for regular application traffic.

  Note: pglogical replicates DDL executed on the provider node, and forces
  execution against fully schema-qualified names internally. Any raw
  `execute/1` calls added to future migrations should be schema-qualified
  to behave identically whether or not this adapter is in use.
  """
  use Ecto.Adapters.SQL, driver: :postgrex

  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Structure

  @impl Ecto.Adapter.Migration
  defdelegate supports_ddl_transaction?, to: Ecto.Adapters.Postgres

  @impl Ecto.Adapter.Migration
  defdelegate lock_for_migrations(meta, opts, fun), to: Ecto.Adapters.Postgres

  @impl Ecto.Adapter.Storage
  defdelegate storage_up(opts), to: Ecto.Adapters.Postgres

  @impl Ecto.Adapter.Storage
  defdelegate storage_down(opts), to: Ecto.Adapters.Postgres

  @impl Ecto.Adapter.Storage
  defdelegate storage_status(opts), to: Ecto.Adapters.Postgres

  @impl Ecto.Adapter.Structure
  defdelegate structure_dump(default, config), to: Ecto.Adapters.Postgres

  @impl Ecto.Adapter.Structure
  defdelegate structure_load(default, config), to: Ecto.Adapters.Postgres

  @impl Ecto.Adapter.Structure
  defdelegate dump_cmd(args, opts, config), to: Ecto.Adapters.Postgres
end
