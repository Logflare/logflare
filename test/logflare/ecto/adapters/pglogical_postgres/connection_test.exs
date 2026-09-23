defmodule Logflare.Ecto.Adapters.PglogicalPostgres.ConnectionTest do
  use ExUnit.Case, async: false

  alias Logflare.Ecto.Adapters.PglogicalPostgres.Connection
  alias Logflare.Repo.Migrator

  setup do
    previous = Application.get_env(:logflare, Migrator)

    on_exit(fn ->
      Application.put_env(:logflare, Migrator, previous)
    end)
  end

  describe "execute_ddl/1" do
    test "wraps CREATE TABLE ddl in pglogical.replicate_ddl_command with all configured sets" do
      Application.put_env(:logflare, Migrator, replication_sets: ["my_set", "other_set"])

      [sql] =
        Connection.execute_ddl(
          {:create, %Ecto.Migration.Table{name: "widgets"},
           [{:add, :id, :bigserial, [primary_key: true]}]}
        )

      sql = IO.iodata_to_binary(sql)

      assert sql =~ "SELECT pglogical.replicate_ddl_command($lf_ddl$"
      assert sql =~ "CREATE TABLE"
      assert sql =~ "ARRAY['my_set', 'other_set']::text[]"
    end

    test "does not wrap ddl when no replication sets are configured" do
      Application.put_env(:logflare, Migrator, replication_sets: [])

      [sql] =
        Connection.execute_ddl(
          {:create, %Ecto.Migration.Table{name: "widgets"},
           [{:add, :id, :bigserial, [primary_key: true]}]}
        )

      sql = IO.iodata_to_binary(sql)

      refute sql =~ "pglogical.replicate_ddl_command"
      assert sql =~ "CREATE TABLE"
    end

    test "does not wrap raw execute/1 SQL strings" do
      Application.put_env(:logflare, Migrator, replication_sets: ["my_set"])

      [sql] = Connection.execute_ddl("ALTER SYSTEM SET wal_level = 'logical'")

      refute sql =~ "pglogical.replicate_ddl_command"
      assert sql =~ "ALTER SYSTEM SET wal_level = 'logical'"
    end

    test "wraps raw execute/1 SQL strings inside with_replicated_execute/1" do
      Application.put_env(:logflare, Migrator, replication_sets: ["my_set"])

      [sql] =
        Migrator.with_replicated_execute(fn ->
          Connection.execute_ddl("CREATE TYPE oban_job_state AS ENUM ('available')")
        end)

      assert sql =~ "SELECT pglogical.replicate_ddl_command($lf_ddl$"
      assert sql =~ "CREATE TYPE oban_job_state AS ENUM ('available')"
      assert sql =~ "ARRAY['my_set']::text[]"
    end

    test "does not wrap raw execute/1 SQL strings inside with_replicated_execute/1 when no replication sets are configured" do
      Application.put_env(:logflare, Migrator, replication_sets: [])

      [sql] =
        Migrator.with_replicated_execute(fn ->
          Connection.execute_ddl("alter table sources replica identity full")
        end)

      assert sql == "alter table sources replica identity full"
    end
  end
end
