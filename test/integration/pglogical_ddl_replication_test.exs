defmodule Logflare.Integration.PglogicalDdlReplicationTest do
  @moduledoc """
  Integration tests verifying that Ecto migration DDL is propagated to a
  pglogical read replica when `LOGFLARE_PGLOGICAL_REPLICATE_DDL_COMMANDS_SETS`
  is configured, and left un-propagated when it is not.

  Requires the pglogical primary/replica containers from
  `docker-compose.pglogical.yml`, bootstrapped via
  `test/support/pglogical/bootstrap.exs`.
  Run with: `mix test --only pglogical_replica`
  """
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :pglogical_replica

  alias Logflare.Repo.Migrator
  alias Logflare.Support.PglogicalPrimaryRepo
  alias Logflare.Support.PglogicalTestMigration
  alias Logflare.Support.PglogicalTestMigrationAlter
  alias Logflare.TestUtils

  @primary_url System.get_env(
                 "PGLOGICAL_PRIMARY_URL",
                 "postgresql://postgres:postgres@localhost:5433/logflare_test"
               )
  @replica_url System.get_env(
                 "PGLOGICAL_REPLICA_URL",
                 "postgresql://postgres:postgres@localhost:5434/logflare_test"
               )
  @replication_set System.get_env("PGLOGICAL_TEST_REPLICATION_SET", "my_set")

  @migration_versions %{
    PglogicalTestMigration => 1,
    PglogicalTestMigrationAlter => 2
  }

  setup_all do
    {:ok, primary_conn} = Postgrex.start_link(Ecto.Repo.Supervisor.parse_url(@primary_url))
    {:ok, replica_conn} = Postgrex.start_link(Ecto.Repo.Supervisor.parse_url(@replica_url))
    {:ok, primary_conn: primary_conn, replica_conn: replica_conn}
  end

  setup %{primary_conn: primary_conn, replica_conn: replica_conn} do
    initial_migrator_env = Application.get_env(:logflare, Migrator)

    on_exit(fn ->
      Application.put_env(:logflare, Migrator, initial_migrator_env)
      Enum.each([Logflare.Repo.Pglogical, PglogicalPrimaryRepo], &stop_repo/1)
    end)

    reset_public_schema!(primary_conn)
    reset_public_schema!(replica_conn)
    :ok
  end

  describe "when replication sets are configured" do
    setup do
      Application.put_env(:logflare, Migrator, replication_sets: [@replication_set])
      :ok
    end

    test "propagates CREATE TABLE DDL to the replica", %{replica_conn: replica_conn} do
      run_migration!(PglogicalTestMigration, :up)

      TestUtils.retry_assert(fn ->
        assert table_exists?(replica_conn, "pglogical_ddl_test")
      end)
    end

    test "propagates ALTER TABLE ADD COLUMN DDL to the replica", %{replica_conn: replica_conn} do
      run_migration!(PglogicalTestMigration, :up)
      run_migration!(PglogicalTestMigrationAlter, :up)

      TestUtils.retry_assert(fn ->
        assert column_exists?(replica_conn, "pglogical_ddl_test", "extra_column")
      end)
    end

    test "propagates DROP TABLE DDL to the replica", %{replica_conn: replica_conn} do
      run_migration!(PglogicalTestMigration, :up)

      TestUtils.retry_assert(fn ->
        assert table_exists?(replica_conn, "pglogical_ddl_test")
      end)

      run_migration!(PglogicalTestMigration, :down)

      TestUtils.retry_assert(fn ->
        refute table_exists?(replica_conn, "pglogical_ddl_test")
      end)
    end
  end

  describe "when replication sets are not configured" do
    setup do
      Application.put_env(:logflare, Migrator, replication_sets: [])
      :ok
    end

    test "does not propagate DDL to the replica", %{replica_conn: replica_conn} do
      assert Migrator.migration_repo_for(Logflare.Repo) == Logflare.Repo

      run_migration!(PglogicalTestMigration, :up)

      refute table_exists?(replica_conn, "pglogical_ddl_test")
    end
  end

  defp run_migration!(migration_module, direction) do
    repo =  case Migrator.migration_repo_for(Logflare.Repo) do
      Logflare.Repo -> PglogicalPrimaryRepo
      pglogical_repo -> pglogical_repo
    end

    unless Process.whereis(repo) do
      {:ok, _} = repo.start_link(url: @primary_url, pool_size: 2)
    end

    # Versions must be stable per migration: `Ecto.Migrator` only runs a `:down`
    # for a version already recorded in `schema_migrations`, so a freshly minted
    # version would make the rollback a silent no-op.
    version = Map.fetch!(@migration_versions, migration_module)
    Ecto.Migrator.run(repo, [{version, migration_module}], direction, all: true)
  end


  defp stop_repo(repo) do
    case Process.whereis(repo) do
      nil ->
        :ok

      pid ->
        # the repo is linked to the (now dead) test process, so it may already
        # be shutting down by the time on_exit runs
        try do
          Supervisor.stop(pid)
        catch
          :exit, _reason -> :ok
        end
    end
  end

  defp table_exists?(conn, table_name) do
    {:ok, %{rows: rows}} =
      Postgrex.query(
        conn,
        "SELECT 1 FROM information_schema.tables WHERE table_name = $1",
        [table_name]
      )

    rows != []
  end

  defp column_exists?(conn, table_name, column_name) do
    {:ok, %{rows: rows}} =
      Postgrex.query(
        conn,
        "SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2",
        [table_name, column_name]
      )

    rows != []
  end

  # Drops everything in `public` rather than named tables so a test never
  # inherits a partially-migrated table, a stale `schema_migrations` row, or a
  # replication-set membership left by a previous run. CASCADE also clears the
  # `pglogical.replication_set_table` rows that `replicate_ddl_command` adds.
  defp reset_public_schema!(conn) do
    Postgrex.query!(
      conn,
      """
      DO $$
      DECLARE
        stmt text;
      BEGIN
        FOR stmt IN
          SELECT format('DROP TABLE IF EXISTS public.%I CASCADE', tablename)
          FROM pg_tables
          WHERE schemaname = 'public'
        LOOP
          EXECUTE stmt;
        END LOOP;
      END
      $$;
      """,
      []
    )
  end
end
